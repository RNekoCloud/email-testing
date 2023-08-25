package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	// "strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/gomail.v2"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     "VP7S6R6YVYVPF7YP",
		"sasl.password":     "o3CvpeIYse9hrUP1NIi2q+lj5sSCgm73Kog6lEdFWfwOHIbt6dtSAYKn1yGH3Bh4",
		"group.id":          "my-group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	topics := []string{"email.topic"}
	consumer.SubscribeTopics(topics, nil)

	// Konfigurasi Mailtrap
	smtpHost := "sandbox.smtp.mailtrap.io"
	smtpPort := 587
	smtpUsername := "bf57fb5110c34c"
	smtpPassword := "a0d0f31be7ef26"

	d := gomail.NewDialer(smtpHost, smtpPort, smtpUsername, smtpPassword)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case sig := <-signals:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Received message on topic %s: %s\n", *e.TopicPartition.Topic, string(e.Value))
				sendEmail(d, string(e.Value))
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
			}
		}
	}
}

func sendEmail(d *gomail.Dialer, message string) {
	m := gomail.NewMessage()
	m.SetHeader("From", message)
	m.SetHeader("To", "recipient@example.com")
	m.SetHeader("Subject", "Kafka Message")
	m.SetBody("text/plain", message)

	if err := d.DialAndSend(m); err != nil {
		log.Printf("Failed to send email: %v", err)
	}
}
