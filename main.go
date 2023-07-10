package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"

	"csv-handler/consumer"
	"csv-handler/rabbitmq"
	"csv-handler/routes"
)

func main() {
	// Load the configuration file
	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Failed to read configuration file: %v", err)
	}

	err := rabbitmq.InitializeRabbitMQ()
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}

	go consumer.StartWorker()

	router := mux.NewRouter()

	// Setup the API routes
	routes.SetupRoutes(router, "/api/v1")

	// Start the server
	log.Fatal(http.ListenAndServe(":8080", router))

}
