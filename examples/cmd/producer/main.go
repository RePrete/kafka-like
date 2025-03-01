package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/RePrete/kafka-like/proto/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Simple configuration
	serverAddr := "localhost:50051"
	topicName := "example-topic"
	messageCount := 5

	// Connect to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := pb.NewQueueServiceClient(conn)
	ctx := context.Background()

	// Create a topic with 3 partitions
	createTopic(ctx, client, topicName, 3)

	// Produce example messages
	for i := 0; i < messageCount; i++ {
		messageText := fmt.Sprintf("Example message #%d sent at %s", i+1, time.Now().Format(time.RFC3339))
		produceMessage(ctx, client, topicName, messageText)

		// Small delay between messages
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Successfully produced %d messages to topic '%s'", messageCount, topicName)
}

func createTopic(ctx context.Context, client pb.QueueServiceClient, name string, partitions int) {
	log.Printf("Creating topic '%s' with %d partitions...", name, partitions)

	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Name:       name,
		Partitions: int32(partitions),
	})

	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	if !resp.Success {
		log.Fatalf("Failed to create topic: %s", resp.Error)
	}

	log.Println("Topic created successfully")
}

func produceMessage(ctx context.Context, client pb.QueueServiceClient, topic, value string) {
	log.Printf("Producing message to topic '%s': %s", topic, value)

	resp, err := client.Produce(ctx, &pb.ProduceRequest{
		Message: &pb.QueueMessage{
			Key:       []byte("example-key"),
			Value:     []byte(value),
			Topic:     topic,
			Partition: 0,
			Headers: []*pb.MessageHeader{
				{
					Key:   "content-type",
					Value: []byte("text/plain"),
				},
			},
		},
	})

	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	if !resp.Success {
		log.Fatalf("Failed to produce message: %s", resp.Error)
	}

	log.Println("Message produced successfully")
}
