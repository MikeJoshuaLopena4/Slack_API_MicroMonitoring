package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/pubsub"
)

type Data struct {
	Date        string  `json:"date"`
	Service     string  `json:"service"`
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
}

var services = []string{"AmazonEC2", "Google Cloud", "Azure", "AWS Lambda", "Heroku"}

const projectID = "alphaus-live"
const topicID = "Shark"

func main() {
	ctx := context.Background()

	// Create Pub/Sub client
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		fmt.Printf("Error creating Pub/Sub client: %v\n", err)
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentDate := time.Now().Format("2006-01-02")
		service := services[rand.Intn(len(services))]
		description := "This is a sample description for " + service
		cost := rand.Float64() * 1000 // Random cost between 0 and 1000
		data := Data{
			Date:        currentDate,
			Service:     service,
			Description: description,
			Cost:        cost,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Println("Error marshalling JSON:", err)
			continue
		}

		// Publish data to Pub/Sub
		err = publishData(ctx, client, jsonData)
		if err != nil {
			fmt.Println("Error publishing data:", err)
		}
	}
}

func publishData(ctx context.Context, client *pubsub.Client, data []byte) error {
	topic := client.Topic(topicID)
	defer topic.Stop()

	result := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	_, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Publish error: %v", err)
	}
	fmt.Println("Data published successfully")
	return nil
}
