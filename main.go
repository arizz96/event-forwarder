package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"encoding/json"
	"net/http"
	"crypto/tls"

	kafka "github.com/segmentio/kafka-go"
	"gopkg.in/segmentio/analytics-go.v3"
)

type TypeMessage struct {
	Type string `json:"type,omitempty"`
}

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
	client, _ := analytics.NewWithConfig(os.Getenv("SEGMENT_WRITE_KEY"), analytics.Config{ Transport: &http.Transport{ TLSClientConfig: &tls.Config{InsecureSkipVerify : true} } })

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

		var result TypeMessage
		json.Unmarshal([]byte(m.Value), &result)

		fmt.Println(result.Type)

		var msg analytics.Message
		switch result.Type {
		case "alias":
			msg = new(analytics.Alias)
		case "group":
			msg = new(analytics.Group)
		case "identify":
			msg = new(analytics.Identify)
		case "page":
			msg = new(analytics.Page)
		case "track":
			msg = new(analytics.Track)
		}

		// Unmarshal to type
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			fmt.Println(err)
			return
		}

		client.Enqueue(msg)
	}
}
