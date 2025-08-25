package kafka

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"log"
	"os"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(kafkaURL),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Async:                  true,
		RequiredAcks:           kafka.RequireAll,
		Balancer:               &kafka.LeastBytes{},
	}
}

func GetEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func KafkaProducer() {
	//producer
	kafkaURL := GetEnv("KAFKA_URL", "localhost:9092")
	topic := GetEnv("KAFKA_TOPIC", "topic")

	writer := newKafkaWriter(kafkaURL, topic)

	defer writer.Close()

	fmt.Println("start producing ... !!")

	b, err := os.ReadFile("../model/model.json")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Отправлено сообщение - ")
	msg := kafka.Message{Value: b}
	err = writer.WriteMessages(context.Background(), msg)

}
