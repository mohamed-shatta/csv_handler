package api

import (
	"bufio"
	"csv-handler/postgres"
	"csv-handler/rabbitmq"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

func HandleGetData(w http.ResponseWriter, r *http.Request) {
	// Parse and extract the filters from the request URL or request body
	filters := parseFilters(r)

	// Get the limit and offset values for pagination
	limit, offset := getPaginationParams(r)

	// Create an instance of the PostgreSQL client
	pgClient, err := postgres.NewClient()
	if err != nil {
		// Handle the error and return an appropriate response
		http.Error(w, "Failed to initialize PostgreSQL client", http.StatusInternalServerError)
		return
	}
	defer pgClient.Close()

	// Call the GetData method to retrieve the data from PostgreSQL
	data, err := pgClient.GetData(filters, limit, offset)
	if err != nil {
		// Handle the error and return an appropriate response
		http.Error(w, "Failed to retrieve data from PostgreSQL", http.StatusInternalServerError)
		return
	}

	// Convert the data to JSON
	// Check if there are no results
	if len(data) == 0 {
		// Return an empty JSON object
		emptyData := make(map[string]interface{})
		jsonData, err := json.Marshal(emptyData)
		if err != nil {
			fmt.Println("Failed to marshal JSON:", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
		return
	}
	responseJSON, err := json.Marshal(data)
	if err != nil {
		// Handle the error and return an appropriate response
		http.Error(w, "Failed to convert data to JSON", http.StatusInternalServerError)
		return
	}

	// Set the response headers and write the JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseJSON)
}

func getPaginationParams(r *http.Request) (int, int) {
	// Extract the limit and offset values from the request URL or request body
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	// Parse and validate the limit and offset values
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 {
		// Handle invalid limit value, return a default value or error
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		// Handle invalid offset value, return a default value or error
	}

	return limit, offset
}

func parseFilters(r *http.Request) map[string]interface{} {
	// Extract the filters from the request URL or request body and parse them as needed
	values := r.URL.Query()

	filters := make(map[string]interface{})

	// Extract and parse the "created_at" filter
	createdAt := values.Get("created_at")
	if createdAt != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["created_at"] = createdAt
	}

	// Extract and parse the "deleted_at" filter
	deletedAt := values.Get("deleted_at")
	if deletedAt != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["deleted_at"] = deletedAt
	}

	// Extract and parse the "email_address" filter
	emailAddress := values.Get("email_address")
	if emailAddress != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["email_address"] = emailAddress
	}

	// Extract and parse the "first_name" filter
	firstName := values.Get("first_name")
	if firstName != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["first_name"] = firstName
	}

	// Extract and parse the "last_name" filter
	lastName := values.Get("last_name")
	if lastName != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["last_name"] = lastName
	}

	// Extract and parse the "id" filter
	id := values.Get("id")
	if id != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["id"] = id
	}

	// Extract and parse the "id" filter
	parentId := values.Get("parent_user_id")
	if parentId != "" {
		// Perform any necessary parsing or validation for the filter value
		// Add the filter to the map
		filters["parent_user_id"] = parentId
	}

	return filters
}

// HandleFileUpload handles the POST /upload endpoint for file upload
func HandleFileUpload(w http.ResponseWriter, r *http.Request) {
	// Retrieve the uploaded file from the request
	file, _, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Failed to retrieve file: %v", err)
		return
	}
	defer file.Close()

	// Create a new RabbitMQ instance
	rabbitMQ := rabbitmq.GetRabbitMQInstance()

	// Create a buffered reader to read the file in chunks
	bufferedReader := bufio.NewReader(file)

	// Set the chunk size (adjust according to your requirements)
	chunkSize := 100
	count := 0

	// Buffer to hold the chunk data
	chunk := make([]byte, chunkSize)

	// Incomplete line buffer
	incompleteLine := ""

	queue_name := viper.GetString("rabbitmq.csv_rabbitmq")

	// Read the first line separately
	firstLine, err := bufferedReader.ReadString('\n')
	if err != nil && err != io.EOF {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to read file: %v", err)
		return
	}
	firstLine = strings.Trim(firstLine, "\ufeff")
	firstLine = strings.TrimSpace(firstLine)
	headers := strings.Split(firstLine, ",")

	// Read and process the CSV data in chunks
	for {
		// Read a chunk from the file
		numBytesRead, err := bufferedReader.Read(chunk)
		if err != nil && err != io.EOF {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to read file: %v", err)
			return
		}

		// Combine incomplete line from previous chunk, if any, with the current chunk
		data := incompleteLine + string(chunk[:numBytesRead])

		// Split the data into lines
		lines := strings.Split(data, "\n")

		// If the last line is incomplete, store it for the next chunk
		lastLine := lines[len(lines)-1]
		if !strings.HasSuffix(data, "\n") {
			incompleteLine = lastLine
			lines = lines[:len(lines)-1] // Exclude the incomplete last line from processing
		} else {
			incompleteLine = "" // Clear the incomplete line buffer
		}

		// Process and publish each line individually
		for _, line := range lines {
			// Skip empty lines
			if line == "" {
				continue
			}

			count = count + 1

			// Split the line into values
			line = strings.ReplaceAll(line, "\r", "")
			values := strings.Split(line, ",")

			// Create an object to hold the line values
			obj := make(map[string]string)

			// Assign values to keys from the first line
			for i, key := range headers {
				obj[key] = values[i]
			}

			// Convert the object to JSON
			jsonData, err := json.Marshal(obj)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Failed to convert object to JSON: %v", err)
				return
			}

			//Publish the line to RabbitMQ
			err = rabbitMQ.Publish(queue_name, string(jsonData))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Failed to publish line to RabbitMQ: %v", err)
				return
			}
		}

		// Check for end of file
		if err == io.EOF {
			break
		}
	}

	// Publish the incomplete last line if it exists
	if incompleteLine != "" {

		// Split the line into values
		incompleteLine = strings.ReplaceAll(incompleteLine, "\r", "")
		values := strings.Split(incompleteLine, ",")

		// Create an object to hold the line values
		obj := make(map[string]string)

		// Assign values to keys from the first line
		for i, key := range headers {
			obj[key] = values[i]
		}

		// Convert the object to JSON
		jsonData, err := json.Marshal(obj)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to convert object to JSON: %v", err)
			return
		}

		err = rabbitMQ.Publish(queue_name, string(jsonData))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to publish line to RabbitMQ: %v", err)
			return
		}
	}

	// File upload and publishing successful
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "File uploaded and processed in chunks, lines published successfully")
}
