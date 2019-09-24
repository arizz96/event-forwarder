package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"encoding/json"

	kafka "github.com/segmentio/kafka-go"
	"./analytics"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	// initialize segment client
	client := analytics.New(os.Getenv("SEGMENT_WRITE_KEY"))

	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("KAFKA_BROKERS_URL")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		var result analytics.Message
		json.Unmarshal([]byte(m.Value), &result)

		client.Enqueue(result)
	}
}
