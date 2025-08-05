package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "anything-test-topic", // topic이 없어도 자동 생성돼야 함
	})

	time.Sleep(10 * time.Second)
	err := w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("k"),
		Value: []byte("hello"),
	})
	if err != nil {
		log.Fatal("❌ Failed to write:", err)
	}

	log.Println("✅ Successfully wrote message to Kafka")
	w.Close()
}
