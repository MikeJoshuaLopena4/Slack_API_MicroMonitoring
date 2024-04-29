package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// SlackMessage represents the structure of the JSON payload to be sent to Slack
type SlackMessage struct {
	Text string `json:"text"`
}

func main() {
	// Infinite loop
	for {
		// Run the main logic
		err := run()
		if err != nil {
			log.Printf("Error: %v", err)
		}

		// Delay time here
		time.Sleep(1 * time.Minute)
	}
}

// run contains the main logic of the program
func run() error {
	ctx := context.Background()

	// Your Cloud Spanner database information
	instanceName := "XXXXXXXXXXXX"
	databaseName := "XXXXXXXXXXXX"
	tableName := "XXXXXXXXXXXX"

	// Create a Spanner client
	client, err := spanner.NewClient(ctx, fmt.Sprintf("XXXXXXXXXXXX", "XXXXXXXXXXXX", instanceName, databaseName))
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()

	// Query data from sample_table
	stmt := spanner.Statement{
		SQL: `SELECT description FROM ` + tableName,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var (
		longest            string
		mostOccurredWord   string
		leastOccurredWords []string
		wordCount          = make(map[string]int)
		totalMessages      int
		hasRows            bool
	)

	// Process each row
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break // No more rows to process
			}
			return fmt.Errorf("error retrieving data: %v", err)
		}

		hasRows = true

		var description string
		if err := row.Columns(&description); err != nil {
			return fmt.Errorf("error retrieving description: %v", err)
		}

		// Determine the current longest description
		if len(description) > len(longest) {
			longest = description
		}

		// Determine the most and least occurring word
		words := strings.Fields(description)
		for _, word := range words {
			wordCount[word]++
			if wordCount[word] > wordCount[mostOccurredWord] {
				mostOccurredWord = word
			}
		}

		totalMessages++
	}

	if !hasRows {
		log.Println("No rows found")
		return nil
	}

	// Find all least occurring words
	minOccurrences := totalMessages
	for word, count := range wordCount {
		if count < minOccurrences {
			minOccurrences = count
			leastOccurredWords = []string{word}
		} else if count == minOccurrences {
			leastOccurredWords = append(leastOccurredWords, word)
		}
	}

	// Construct the message to be printed in the terminal and sent to Slack
	message := fmt.Sprintf("Longest description: %s\nMost occurring word: %s\nLeast occurring word(s): %s\nNumber of messages processed: %d",
		longest, mostOccurredWord, strings.Join(leastOccurredWords, ", "), totalMessages)

	// Print the message in the terminal
	fmt.Println(message)

	// Send the message to Slack
	err = sendToSlack(message)
	if err != nil {
		return fmt.Errorf("error sending message to Slack: %v", err)
	}

	return nil
}

// sendToSlack sends a message to Slack using the webhook URL
func sendToSlack(message string) error {
	// Replace 'YOUR_WEBHOOK_URL' with the actual webhook URL
	webhookURL := "XXXXXXXXXXXX"

	// Create a SlackMessage with the text
	slackMessage := SlackMessage{
		Text: message,
	}

	// Convert the SlackMessage struct to JSON
	jsonMessage, err := json.Marshal(slackMessage)
	if err != nil {
		return err
	}

	// Send the JSON payload to Slack using HTTP POST request
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonMessage))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error sending message to Slack: %s", resp.Status)
	}

	return nil
}
