package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkaLib "github.com/segmentio/kafka-go"
)

// Client wraps kafka.Writer for producing messages
type Client struct {
	writer *kafkaLib.Writer
}

// Message[T] is a generic Kafka message
type Message[T any] struct {
	Key   []byte
	Value T
}

// NewClient initializes a new Kafka writer
func NewClient(brokers []string, topic string) *Client {
	return &Client{
		writer: &kafkaLib.Writer{
			Addr:         kafkaLib.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkaLib.LeastBytes{},
			RequiredAcks: kafkaLib.RequireOne,
			Async:        false,
			BatchTimeout: 200 * time.Millisecond,
		},
	}
}

// PublishBatch is a standalone generic function for publishing batches
// 제너릭 강제하는 과정에서 메서드 대신 함수로 썼음
func PublishBatch[T any](ctx context.Context, writer *kafkaLib.Writer, msgs []Message[T]) error {
	var kafkaMsgs []kafkaLib.Message

	for _, m := range msgs {
		data, err := json.Marshal(m.Value)
		if err != nil {
			return err
		}
		kafkaMsgs = append(kafkaMsgs, kafkaLib.Message{
			Key:   m.Key,
			Value: data,
		})
	}

	return writer.WriteMessages(ctx, kafkaMsgs...)
}

// Writer returns the underlying kafka.Writer (helper accessor)
func (c *Client) Writer() *kafkaLib.Writer {
	return c.writer
}

// Close closes the Kafka writer
func (c *Client) Close() error {
	return c.writer.Close()
}

// NewReader creates a new Kafka consumer for reading messages
func NewReader(brokers []string, topic string, groupID string) *kafkaLib.Reader {
	return kafkaLib.NewReader(kafkaLib.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
}
