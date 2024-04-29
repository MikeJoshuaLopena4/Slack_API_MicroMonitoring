package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
)

var (
	subId  = flag.String("subscription", "Shark-sub", "Subscription name")
	ticker = time.NewTicker(1 * time.Minute)
)

// Message struct to represent the JSON message format
type Message struct {
	Date        string  `json:"Date"`
	Service     string  `json:"Service"`
	Description string  `json:"Description"`
	Cost        float64 `json:"Cost"`
}

// Function to send message to Slack
func sendToSlack(message string) {
	// Define your Slack webhook URL
	webhookURL := "https://hooks.slack.com/services/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"

	// Create a payload containing the message
	payload, err := json.Marshal(map[string]string{
		"text": message,
	})
	if err != nil {
		log.Println("JSON Marshal failed:", err)
		return
	}
	// Send the payload to Slack using an HTTP POST request
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Println("HTTP POST request failed:", err)
		return
	}
	defer resp.Body.Close()
	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		log.Printf("HTTP POST request failed with status code: %d", resp.StatusCode)
		return
	}
	log.Println("Message sent successfully to Slack channel")
}
func main() {
	flag.Parse()
	projectId := "alphaus-live"
	ctx := context.Background()
	if *subId == "" {
		log.Println("subscription cannot be empty")
		return
	}
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		log.Println("NewClient failed:", err)
		return
	}
	defer client.Close()
	sub := client.Subscription(*subId)
	sub.ReceiveSettings.Synchronous = true
	// Receive blocks until the context is cancelled or an error occurs.
	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		// Decode the JSON message into a Message struct
		var m Message
		err := json.Unmarshal(msg.Data, &m)
		if err != nil {
			log.Println("JSON unmarshal failed:", err)
			msg.Nack()
			return
		}
		// Print the message ID and the fields of the Message struct
		log.Printf("Received message ID: %s\n", msg.ID)
		log.Printf("Message content: Date: %s, Service: %s, Description: %s, Cost: %.2f\n", m.Date, m.Service, m.Description, m.Cost)
		// Send the message to Slack
		slackMessage := "Message ID: " + msg.ID + "\n" +
			"Date: " + m.Date + "\n" +
			"Service: " + m.Service + "\n" +
			"Description: " + m.Description + "\n" +
			"Cost: " + fmt.Sprintf("%.2f", m.Cost)

		// Add a delay before sending to Slack
		<-ticker.C // rate limit to 1 msg/min
		sendToSlack(slackMessage)
		// Acknowledge the message
		msg.Ack()
	})
	if err != nil {
		log.Println("Receive failed:", err)
		return
	}
}
