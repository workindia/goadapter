package producer

import (
	"reflect"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	// Name of the topic
	Topic string
	// Array of the kafka brokers URL
	Brokers []string
	// Writetimeout for connection
	// Default is 10 seconds
	WriteTimeout int
	// Readtimeout for connection
	// Default is 10 seconds
	ReadTimeout int
	// Push of message to be sync or async
	Async bool
	// No of partitions. Default will be one
	// This will be used only while creating the topic
	NumPartitions int
	// Replication Factor. Default will be one.
	// This will be used only while creating the topic
	ReplicationFactor int
}

// Converting ProducerConfig to Segmentio's WriteConfig
// This is need to create abstraction from Segmentio's config. Dev should not be
// worried about setting Dialer or CompressionCodec in WriterConfig
func (config *ProducerConfig) GetKafkaWriterConfig() *kafka.WriterConfig {
	writeConfig := kafka.WriterConfig{
		Brokers:      config.Brokers,
		Topic:        config.Topic,
		WriteTimeout: time.Duration(config.WriteTimeout) * time.Second,
		ReadTimeout:  time.Duration(config.ReadTimeout) * time.Second,
		Async:        config.Async,
	}
	return &writeConfig
}

// Overwrite kafka.WriterConfig, any parameter with required value
// This should not be needed under normal circumstances.
// Example will be changing Balancer to something else that round robin
func OverwriteWriterConfig(config *kafka.WriterConfig, name string, value interface{}) bool {
	val := reflect.ValueOf(config)
	_struct := val.Elem()
	if _struct.Kind() == reflect.Struct {
		field := _struct.FieldByName(name)
		if field.IsValid() && field.CanSet() {
			field.Set(reflect.ValueOf(value))
			return true
		}
	}
	return false
}
