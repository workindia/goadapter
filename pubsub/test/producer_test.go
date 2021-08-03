package pubsub

import (
	"context"
	"testing"

	"github.com/workindia/goadapter/pubsub/producer"
)

type SampleMessage struct {
	ProducerID   int
	ProducerName string
	ProducerFees float32
}

type TestProducer struct {
	Producer producer.BaseProducer
}

// must start kafka on 9092 port

func TestProduce(t *testing.T) {
	TestProducerConfig := producer.ProducerConfig{
		Brokers:      []string{"127.0.0.1:9092"},
		Topic:        "my-funny-topic",
		WriteTimeout: 10,
		ReadTimeout:  10,
		Async:        false,
	}
	TestPorducerObj := TestProducer{
		Producer: producer.BaseProducer{Config: TestProducerConfig},
	}
	message := SampleMessage{
		ProducerID:   12,
		ProducerName: "Tim",
		ProducerFees: 499.99,
	}
	err := producer.Produce(context.Background(), &TestPorducerObj.Producer, message)
	if err != nil {
		t.Log("Produce call failed for message.")
		t.Fail()
	}
}
