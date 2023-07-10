package consumer

import (
	"csv-handler/postgres"
	"csv-handler/rabbitmq"
	redisclient "csv-handler/redis"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// StartWorker starts the consumer worker to consume messages
func StartWorker() {
	// Get the RabbitMQ instance
	rabbitMQ := rabbitmq.GetRabbitMQInstance()

	// Create a new PostgreSQL client
	pgClient, err := postgres.NewClient()
	if err != nil {
		log.Fatalf("Failed to initialize PostgreSQL client: %v", err)
	}
	defer pgClient.Close()

	// Create a new Redis client
	rdb, err := redisclient.NewClient()
	if err != nil {
		fmt.Println("Failed to create Redis client:", err)
		return
	}
	defer rdb.Close()

	// Declare and publish to Queue1
	err = rabbitMQ.DeclareQueue(viper.GetString("rabbitmq.csv_rabbitmq"))
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Create a channel to receive delivery notifications
	deliveryChan, err := rabbitMQ.Consume(viper.GetString("rabbitmq.csv_rabbitmq"))
	if err != nil {
		log.Println("Failed to start consumer:", err)
		return
	}

	// Start processing messages
	for delivery := range deliveryChan {
		// Process the message
		err := processMessage(delivery.Body, pgClient, rdb)
		if err != nil {
			log.Println("Failed to process message:", err)

			// Nack the message
			err := delivery.Nack(false, false)
			if err != nil {
				log.Println("Failed to nack message:", err)
			}

			continue
		}

		//Explicitly acknowledge the message
		err = delivery.Ack(false)
		if err != nil {
			log.Println("consumer Failed to acknowledge message:", err)
		}
	}
}

func processMessage(message []byte, pgClient *postgres.Client, myredis *redisclient.Client) error {
	str := string(message)

	// Remove escape characters from the string
	str = strings.ReplaceAll(str, `\"`, `"`)

	// Remove surrounding double quotes
	str = strings.Trim(str, `"`)

	// Create a variable to hold the JSON data
	var data map[string]interface{}

	// Unmarshal the string into the data variable
	err := json.Unmarshal([]byte(str), &data)
	if err != nil {
		//fmt.Println("Failed to unmarshal JSON:", err)
		return fmt.Errorf("Failed to unmarshal JSON: %w", err)
	}

	// Insert the data into PostgreSQL
	err = pgClient.InsertCsvData(data)
	if err != nil {
		return fmt.Errorf("Failed to insert data into PostgreSQL: %w", err)
	}

	// // Set a key-value pair
	err = myredis.Set(data["id"].(string), str, time.Hour)
	if err != nil {
		fmt.Println("Failed to set key-value pair:", err)
	}
	// Add the JSON member to the sorted set with a score
	// f, err := strconv.ParseFloat(data["id"].(string), 64)
	// if err != nil {
	// 	fmt.Println("Failed to convert string to float64:", err)
	// }
	// err = myredis.ZAdd("csv_data", f, str)
	// if err != nil {
	// 	log.Fatalf("Failed to add member to sorted set: %v", err)
	// }
	return nil
}
