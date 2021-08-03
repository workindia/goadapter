package producer

import (
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type BaseProducer struct {
	Config ProducerConfig
}

func (b *BaseProducer) GetConfig() *ProducerConfig {
	return &b.Config
}

// Dial a connection to the broker
// Must defer conn.Close() after getConnection()
func (b *BaseProducer) getConnection() *kafka.Conn {
	brokers := b.Config.Brokers
	if len(brokers) < 1 {
		panic("At least one broker required")
	}
	conn, err := kafka.Dial(TCP, brokers[0])
	if err != nil {
		panic(err.Error())
	}
	return conn
}

// Dial a connection to leader broker
// Must defer conn.Close() after getConnection()
func (b *BaseProducer) getLeaderConnection() *kafka.Conn {
	conn := b.getConnection()
	defer conn.Close()
	leader, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	leaderConnection, err := kafka.Dial(TCP, net.JoinHostPort(leader.Host, strconv.Itoa(leader.Port)))
	if err != nil {
		panic(err.Error())
	}
	return leaderConnection
}

// Get a writer for kafka message
func (b *BaseProducer) GetWriter() *kafka.Writer {
	writerConfig := b.Config.GetKafkaWriterConfig()
	writer := kafka.NewWriter(*writerConfig)
	return writer
}
