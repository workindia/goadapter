package producer

import (
	"context"
	"fmt"

	"github.com/workindia/goadapter/pubsub"

	"github.com/segmentio/kafka-go"
)

// Constant to Error mapping to recover from panic
var ErrorStrings map[int]string = map[int]string{
	3: "[3] Unknown Topic Or Partition: the request is for a topic or partition that does not exist on this broker",
}

const TCP = "tcp"

var AUTO_TOPIC_CREATION bool = true

type BaseProducerInterface interface {
	GetConfig() *ProducerConfig
	GetWriter() *kafka.Writer
	getLeaderConnection() *kafka.Conn
	getConnection() *kafka.Conn
}

// Synchronously write to the kafka stream. We always write to the leader
// @params:
// 		ctx: Context. While using Gin, pass Gin's context or background of goroutine
//		baseProducer: Producer class
//		message: Message to be sent
// @returns:
//		error: Error if any
func Produce(ctx context.Context, baseProducer BaseProducerInterface, message pubsub.MessageInterface) error {
	writer := baseProducer.GetWriter()
	defer writer.Close()

	kafkaMessage := kafka.Message{
		Key:   nil,
		Value: pubsub.ToCompatibleMessage(message),
	}

	err := writer.WriteMessages(
		ctx,
		kafkaMessage,
	)
	if err != nil {
		recoveryError := recoverFromError(ctx, writer, kafkaMessage, err, baseProducer)
		return recoveryError
	}
	return err
}

// Create a kafka topic for the given producer. Called only while recovering from error
// @params:
//		baseProducer: Producer class
// @returns:
//		error: Error if any
func createKafkaTopic(baseProducer BaseProducerInterface) error {
	leaderConnection := baseProducer.getLeaderConnection()
	defer leaderConnection.Close()
	partitions := 1
	replicationFactor := 1
	if baseProducer.GetConfig().NumPartitions != 0 {
		partitions = baseProducer.GetConfig().NumPartitions
	}
	if baseProducer.GetConfig().ReplicationFactor != 0 {
		replicationFactor = baseProducer.GetConfig().ReplicationFactor
	}
	topicConfig := kafka.TopicConfig{
		Topic:             baseProducer.GetConfig().Topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}
	err := leaderConnection.CreateTopics(topicConfig)
	return err
}

//====================================
//  Kafka Topic Push Failed Recovery
//====================================
// Try to recover from the error which might occur while pushing the message to kafka
func recoverFromError(ctx context.Context, writer *kafka.Writer, kafkaMessage kafka.Message, err error, baseProducer BaseProducerInterface) error {

	switch err.Error() {

	// Topic does not exist on broker
	case ErrorStrings[3]:
		if AUTO_TOPIC_CREATION {
			topic := baseProducer.GetConfig().Topic
			fmt.Printf("Topic %v will be created.\n", topic)
			_err := createKafkaTopic(baseProducer)
			if _err != nil {
				panic(_err.Error())
			}
		}
	}

	_err := writer.WriteMessages(
		ctx,
		kafkaMessage,
	)
	return _err
}
