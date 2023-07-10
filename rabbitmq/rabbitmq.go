package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

var (
	rabbitMQInstance *RabbitMQ
	rabbitMQLock     sync.Mutex
	rabbitMQInit     sync.Once
)

// RabbitMQ represents a RabbitMQ client
type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

// NewRabbitMQ creates a new instance of RabbitMQ
func NewRabbitMQ(amqpURI, username, password string) (*RabbitMQ, error) {
	conn, err := amqp.DialConfig(amqpURI, amqp.Config{
		SASL: []amqp.Authentication{
			&amqp.PlainAuth{
				Username: username,
				Password: password,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
	}

	return &RabbitMQ{
		connection: conn,
		channel:    ch,
	}, nil
}

// GetRabbitMQInstance returns the singleton instance of RabbitMQ
func GetRabbitMQInstance() *RabbitMQ {
	if rabbitMQInstance != nil {
		return rabbitMQInstance
	}

	rabbitMQLock.Lock()
	defer rabbitMQLock.Unlock()

	if rabbitMQInstance == nil {
		rabbitMQInit.Do(func() {
			// Initialize RabbitMQ
			err := InitializeRabbitMQ()
			if err != nil {
				log.Fatalf("Failed to initialize RabbitMQ: %v", err)
			}
		})
	}

	return rabbitMQInstance
}

// InitializeRabbitMQ initializes the singleton instance of RabbitMQ
func InitializeRabbitMQ() error {
	// Read RabbitMQ credentials from the configuration
	rabbitMQHost := viper.GetString("rabbitmq.host")
	rabbitMQUsername := viper.GetString("rabbitmq.username")
	rabbitMQPassword := viper.GetString("rabbitmq.password")

	rabbitMQLock.Lock()
	defer rabbitMQLock.Unlock()

	if rabbitMQInstance != nil {
		return fmt.Errorf("RabbitMQ already initialized")
	}

	rabbitMQ, err := NewRabbitMQ(rabbitMQHost, rabbitMQUsername, rabbitMQPassword)
	if err != nil {
		return err
	}
	rabbitMQInstance = rabbitMQ
	return nil
}

func (r *RabbitMQ) CheckConnection() error {
	if r.connection == nil || r.connection.IsClosed() {
		// Reestablish the RabbitMQ connection
		err := InitializeRabbitMQ()
		if err != nil {
			return fmt.Errorf("failed to reconnect to RabbitMQ: %v", err)
		}
	}
	return nil
}

// DeclareQueue declares a new queue in RabbitMQ
func (r *RabbitMQ) DeclareQueue(queueName string) error {
	q, err := r.channel.QueueDeclare(
		queueName, // Name of the queue
		false,     // Durable (queue survives server restart)
		false,     // Delete when unused
		false,     // Exclusive (for this connection only)
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare RabbitMQ queue: %v", err)
	}

	r.queue = q
	return nil
}

// Publish sends a message to the RabbitMQ queue
func (r *RabbitMQ) Publish(routingKey string, payload interface{}) error {
	err := r.CheckConnection()
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %v", err)
	}

	err = r.channel.Publish(
		"",         // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonPayload,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	return nil
}

// Consume consumes messages from the RabbitMQ queue with manual acknowledgment
func (r *RabbitMQ) Consume(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := r.channel.Consume(
		queueName, // Name of the queue
		"",        // Consumer name (empty string for auto-generated name)
		false,     // Auto-acknowledgment set to false
		false,     // Exclusive (for this connection only)
		false,     // No-local (do not consume own messages)
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to consume from RabbitMQ queue: %v", err)
	}
	return msgs, nil
}

// Ack acknowledges a message to RabbitMQ
func (r *RabbitMQ) Ack(deliveryTag uint64) error {
	err := r.channel.Ack(deliveryTag, false)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %v", err)
	}
	return nil
}

// Nack sends a negative acknowledgment to RabbitMQ, rejecting the message(s)
func (r *RabbitMQ) Nack(deliveryTag uint64, multiple bool, requeue bool) error {
	err := r.channel.Nack(deliveryTag, multiple, requeue)
	if err != nil {
		return fmt.Errorf("failed to send negative acknowledgment: %v", err)
	}
	return nil
}

// Close closes the RabbitMQ connection and channel
func (r *RabbitMQ) Close() {
	if r.channel != nil {
		r.channel.Close()
	}

	if r.connection != nil {
		r.connection.Close()
	}
}
