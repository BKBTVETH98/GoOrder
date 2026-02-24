package main

import (
	"OrderGo/internal/kafka"
	"fmt"
)

func main() {

	fmt.Println("Запускается Producer...")
	kafka.KafkaProducer()

	fmt.Println("\nЗапускается Consumer...")
	kafka.KafkaConsumer()

}
