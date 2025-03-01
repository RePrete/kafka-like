package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/RePrete/kafka-like/proto/gen/proto"
	"github.com/RePrete/kafka-like/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()

	// Create a listener on the specified port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new queue manager
	qm := server.NewQueueManager()

	// Create the gRPC server
	grpcServer := grpc.NewServer()

	// Create and register our service
	queueServer := server.NewGRPCServer(qm)
	proto.RegisterQueueServiceServer(grpcServer, queueServer)

	// Register reflection service on gRPC server
	reflection.Register(grpcServer)

	// Create a channel to listen for OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Create a context that will be canceled when we receive a signal
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine to watch for signals
	go func() {
		sig := <-sigs
		log.Printf("Received signal: %v", sig)
		cancel()
	}()

	// Start a goroutine to run the server
	go func() {
		log.Printf("Starting gRPC server on port %d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for context to be canceled (signal received)
	<-ctx.Done()

	// Gracefully stop the server
	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()

	// Close the queue server
	if err := queueServer.Close(); err != nil {
		log.Printf("Error closing queue server: %v", err)
	}

	log.Println("Server shutdown complete")
}
