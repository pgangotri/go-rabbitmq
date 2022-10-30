package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/streadway/amqp"
)

type Conn struct {
	Channel *amqp.Channel
}

/**
 * Publisher method
 * @param {string} exchange
 * @param {string} routingKey
 * @param {[]byte} data
 */
func (conn Conn) Publish(exchange string, routingKey string, data []byte) error {
	return conn.Channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: data,
	})
}

/**
 * Consumer method
 * @param {string} queue
 * @param {function} handler to handle consumed message
 * @param {int} concurrency no of concurrent consumer to register
 */
func (conn Conn) Consume(queue string, handler func(d amqp.Delivery) bool, concurrency int) error {
	prefetchCount := concurrency * 4;
	err := conn.Channel.Qos(prefetchCount, 0, false)
	if err != nil {
		return err
	}

	msgs, err := conn.Channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for i := 0; i < concurrency; i++ {
		log.Printf("Consuming msg on thread %v : \n", i)
		go func ()  {
			for msg := range msgs {
				if handler(msg) {
					msg.Ack(true)
				} else {
					msg.Ack(false)
				}
			}
			fmt.Println("RabbitMq consumer closed: critical error")
			os.Exit(1)
		}()
	}
	return nil
}

// Create rabbitMq connection
func ConnectToRabbitMq() (Conn, error) {
	url := "amqp://username:password@host:port"
	conn, err := amqp.Dial(url)

	if err != nil {
		return Conn{}, err
	}	

	chann, err := conn.Channel()
	return Conn{
		Channel: chann,
	}, err
}

