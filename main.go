package main

import (
	"encoding/json"
	"fmt"

	amqp "github.com/streadway/amqp"
)

func handler(d amqp.Delivery) bool {
	if d.Body == nil {
		fmt.Println("Error, no message body!")
		return false
	}
	var data map[string]string
	json.Unmarshal(d.Body, &data)
	fmt.Println(data["message"])
	return true
}

func main() {

	eventData := map[string]string{"message": "Test message"}
	eventDataStr, err := json.Marshal(eventData) // convert map to json string
	if err != nil {
		fmt.Println("Error in data conversion")
		fmt.Scanln()
	}

	conn, err := ConnectToRabbitMq() // Initiating new rabbitMq connection
	if err != nil {
		panic(err)
	}

	err = conn.Publish("workerExchange", "publish-golang-event", []byte(eventDataStr)) // publshing json string as bytes stream
	if err != nil {
		fmt.Println("Error in data publishing")
	}


	err = conn.Consume("test-golang-connection", handler, 2)
	if err != nil {
		panic(err)
	}
}