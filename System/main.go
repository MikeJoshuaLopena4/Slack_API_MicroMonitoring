package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	projectID  = "XXXXXXXXXXXX"
	instanceID = "XXXXXXXXXXXX"
	databaseID = "XXXXXXXXXXXX"
	tableName  = "XXXXXXXXXXXX"
	//change according to your credentials
)

type rowData struct {
	ID          int64
	Service     string
	Description string
	Cost        float64
	Date        string
}

type templateData struct {
	Rows                []rowData
	LongestDescription  string
	MostOccurringWord   string
	LeastOccurringWords []string
	MessageCount        int
}

// SlackMessage represents the structure of the JSON payload to be sent to Slack
type SlackMessage struct {
	Text string `json:"text"`
}

func main() {
	// Define HTTP handlers
	http.HandleFunc("/", serveHomePage)
	http.HandleFunc("/template.html", serveMainPage)
	http.HandleFunc("/desc.html", serveDescPage)
	http.HandleFunc("/team.html", serveTeamPage)
	// Serve static files from the "static" directory
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	// Start the HTTP server
	serverAddress := ":8080"
	log.Println("Listening on http://localhost" + serverAddress + ". Your Octo is ready!")
	go func() {
		if err := http.ListenAndServe(serverAddress, nil); err != nil {
			log.Fatal(err)
		}
	}()

	// Open the browser window
	go openBrowser("http://localhost" + serverAddress)

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

func openBrowser(url string) {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Printf("Failed to open browser: %v", err)
	}
}

func serveMainPage(w http.ResponseWriter, r *http.Request) {
	rowDataList, err := fetchData()
	if err != nil {
		handleError(w, err, "Failed to fetch data")
		return
	}
	// Parse template files
	tmpl, err := template.ParseFiles("static/template.html")
	if err != nil {
		handleError(w, err, "Failed to parse template files")
		return
	}
	// Execute template for template.html
	err = tmpl.Execute(w, templateData{
		Rows: rowDataList,
	})
	if err != nil {
		handleError(w, err, "Failed to execute template")
		return
	}
}

func serveDescPage(w http.ResponseWriter, r *http.Request) {
	rowDataList, err := fetchData()
	if err != nil {
		handleError(w, err, "Failed to fetch data")
		return
	}
	longestDescription, mostOccurringWord, leastOccurredWords, messageCount := analyzeData(rowDataList)

	// Parse and execute desc.html template
	tmpl, err := template.ParseFiles("static/desc.html")
	if err != nil {
		handleError(w, err, "Failed to parse template files")
		return
	}
	// Execute template for desc.html
	err = tmpl.Execute(w, templateData{
		Rows:                rowDataList,
		LongestDescription:  longestDescription,
		MostOccurringWord:   mostOccurringWord,
		LeastOccurringWords: leastOccurredWords, // Convert to a slice containing the single word
		MessageCount:        messageCount,
	})
	if err != nil {
		handleError(w, err, "Failed to execute template")
		return
	}
}

func serveTeamPage(w http.ResponseWriter, r *http.Request) {
	// Parse and execute team.html template
	tmpl, err := template.ParseFiles("static/team.html")
	if err != nil {
		handleError(w, err, "Failed to parse template files")
		return
	}
	// Execute template for team.html
	err = tmpl.Execute(w, nil)
	if err != nil {
		handleError(w, err, "Failed to execute template")
		return
	}
}

func serveHomePage(w http.ResponseWriter, r *http.Request) {
	// Parse and execute team.html template
	tmpl, err := template.ParseFiles("static/home.html")
	if err != nil {
		handleError(w, err, "Failed to parse template files")
		return
	}
	// Execute template for team.html
	err = tmpl.Execute(w, nil)
	if err != nil {
		handleError(w, err, "Failed to execute template")
		return
	}
}

func handleError(w http.ResponseWriter, err error, message string) {
	log.Printf("%s: %v\n", message, err)
	http.Error(w, fmt.Sprintf("%s: %v", message, err), http.StatusInternalServerError)
}

func fetchData() ([]rowData, error) {
	// Initialize Spanner client
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("XXXXXXXXXXXX", projectID, instanceID, databaseID),
		option.WithCredentialsFile(`link_to_your_file.json`)) //Replace 'link_to_your_file.json' to your path directory credential
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}
	defer client.Close()
	// Query data from Spanner
	rows := client.Single().Read(
		ctx, tableName, spanner.AllKeys(), []string{"id", "date", "service", "description", "cost"})
	defer rows.Stop()
	var rowDataList []rowData
	// Process and render data as HTML
	for {
		row, err := rows.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate through rows: %v", err)
		}
		var id int64
		var date, service, description string
		var cost float64
		if err := row.Columns(&id, &date, &service, &description, &cost); err != nil {
			return nil, fmt.Errorf("failed to get row columns: %v", err)
		}
		rowDataList = append(rowDataList, rowData{
			ID:          id,
			Date:        date,
			Service:     service,
			Description: description,
			Cost:        cost,
		})
	}
	return rowDataList, nil
}

func analyzeData(data []rowData) (string, string, []string, int) {
	var longestDescription string
	wordCount := make(map[string]int)
	messageCount := len(data) // Assumes that each row represents a message
	mostOccurredWord := ""
	mostOccurredWordCount := 0
	leastOccurredWords := []string{}
	leastOccurredWordCount := int(^uint(0) >> 1) // Initialize to max int value

	// Iterate through the fetched rows to gather additional details
	for _, row := range data {
		description := row.Description
		// Update longest description
		if len(description) > len(longestDescription) {
			longestDescription = description
		}
		// Update word count
		words := strings.Fields(description)
		for _, word := range words {
			wordCount[word]++
			// Find the most occurring word
			if wordCount[word] > mostOccurredWordCount {
				mostOccurredWord = word
				mostOccurredWordCount = wordCount[word]
			}
		}
	}

	// Determine the least occurring word(s)
	for word, count := range wordCount {
		if count < leastOccurredWordCount {
			leastOccurredWords = []string{word}
			leastOccurredWordCount = count
		} else if count == leastOccurredWordCount {
			leastOccurredWords = append(leastOccurredWords, word)
		}
	}

	return longestDescription, mostOccurredWord, leastOccurredWords, messageCount
}

// Slack Notification
// run contains the main logic of the program
func run() error {
	ctx := context.Background()

	// Your Cloud Spanner database information
	instanceName := "intern2024ft"
	databaseName := "default"
	tableName := "sample_table"

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
