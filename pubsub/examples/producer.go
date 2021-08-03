package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/workindia/goadapter/pubsub/producer"
)

// Create a struct for your Producer. Add BaseProducer as its member.
type SampleProducer struct {
	Producer producer.BaseProducer
}

// Create a config for your Producer and Producer
var SampleProducerConfig producer.ProducerConfig
var SampleProducerObj SampleProducer

// Implement init function which will initialize your producer config
func init() {
	SampleProducerConfig = producer.ProducerConfig{
		Brokers:      []string{"127.0.0.1:9092"}, // Read this from a config
		Topic:        "my-funny-topic",
		WriteTimeout: 10,
		ReadTimeout:  10,
		Async:        false,
	}
	// update your Producer Config before here
	SampleProducerObj = SampleProducer{
		Producer: producer.BaseProducer{Config: SampleProducerConfig},
	}
}

// Create a Message type which is to be sent down the stream
// All public + JSON key added attributes will be sent
type SampleProducerMessage struct {
	ProducerID   int32           `json:"producer_id"`
	ProducerName string          `json:"producer_name"`
	ProducerFees float32         `json:"producer_fees"`
	ProducerData json.RawMessage `json:"producer_data"`
}

func main() {
	msg := SampleProducerMessage{
		ProducerID:   12,
		ProducerName: "abcd",
		ProducerFees: 12.99,
		ProducerData: nil,
	}
	err := producer.Produce(context.Background(), &SampleProducerObj.Producer, msg)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Pushed successfully.")
	}
}
