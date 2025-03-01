package main

import (
	"context"
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
	groupID := "example-group"

	// Connect to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := pb.NewQueueServiceClient(conn)
	ctx := context.Background()

	// Subscribe to topic
	log.Printf("Subscribing to topic '%s' with group ID '%s'", topicName, groupID)
	consumerID := subscribe(ctx, client, groupID, []string{topicName})
	log.Printf("Subscribed with consumer ID: %s", consumerID)

	// Consume messages in a simple loop
	messageCount := 0
	maxMessages := 10

	log.Println("Starting to consume messages...")
	for messageCount < maxMessages {
		// Try to consume a message
		message := consumeMessage(ctx, client, consumerID)

		// If we got a message, commit it
		if message != nil {
			commitMessage(ctx, client, consumerID, message)
			messageCount++
		}

		// Small delay to avoid hammering the server
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Consumed %d messages, exiting", messageCount)
}

func subscribe(ctx context.Context, client pb.QueueServiceClient, groupID string, topics []string) string {
	resp, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		GroupId: groupID,
		Topics:  topics,
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	if !resp.Success {
		log.Fatalf("Failed to subscribe: %s", resp.Error)
	}

	return resp.ConsumerId
}

func consumeMessage(ctx context.Context, client pb.QueueServiceClient, consumerID string) *pb.QueueMessage {
	resp, err := client.Consume(ctx, &pb.ConsumeRequest{
		ConsumerId: consumerID,
		TimeoutMs:  5000, // 5 seconds
	})

	if err != nil {
		log.Printf("Error consuming message: %v", err)
		return nil
	}

	if resp.Error != "" {
		if resp.Error != "no message available" { // Don't log normal timeout
			log.Printf("Error from server: %s", resp.Error)
		}
		return nil
	}

	log.Printf("Consumed message: %s", string(resp.Message.Value))
	return resp.Message
}

func commitMessage(ctx context.Context, client pb.QueueServiceClient, consumerID string, message *pb.QueueMessage) {
	commitResp, err := client.Commit(ctx, &pb.CommitRequest{
		ConsumerId: consumerID,
		Message:    message,
	})

	if err != nil {
		log.Printf("Failed to commit message: %v", err)
		return
	}

	if !commitResp.Success {
		log.Printf("Failed to commit message: %s", commitResp.Error)
		return
	}

	log.Printf("Committed message offset: %d", message.Offset)
}
