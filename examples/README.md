# Kafka-like gRPC Client Examples

This directory contains examples of how to use the Kafka-like messaging system with gRPC clients.

## Overview

The examples demonstrate how to:
- Create topics
- Produce messages to topics
- Consume messages using both polling and streaming approaches
- Commit consumed messages

## Available Examples

1. **Original Combined Example**: `grpc_client_example.go` - A single application that demonstrates both producing and consuming in one process.

2. **Separate Producer and Consumer**:
   - Producer: `cmd/producer/main.go` - A standalone producer application
   - Consumer: `cmd/consumer/main.go` - A standalone consumer application

## Building the Examples

To build the separate producer and consumer applications:

```bash
# Build the producer
go build -o producer ./examples/cmd/producer

# Build the consumer
go build -o consumer ./examples/cmd/consumer
```

## Running the Producer

The producer application supports the following command-line flags:

```
--addr string        The server address in the format of host:port (default "localhost:50051")
--timeout duration   Timeout for connections (default 10s)
--secure             Use TLS connection (default false)
--topic string       Topic to produce messages to (default "example-topic")
--partitions int     Number of partitions for topic creation (default 3)
--count int          Number of messages to produce (default 10)
--interval duration  Interval between messages (default 1s)
--create             Create the topic if it doesn't exist (default false)
```

Example usage:

```bash
# Produce 5 messages to the default topic with 2-second intervals
./producer --count 5 --interval 2s

# Create a new topic and produce messages to it
./producer --topic my-new-topic --create --partitions 5 --count 20
```

## Running the Consumer

The consumer application supports the following command-line flags:

```
--addr string        The server address in the format of host:port (default "localhost:50051")
--timeout duration   Timeout for connections (default 10s)
--secure             Use TLS connection (default false)
--topics string      Comma-separated list of topics to consume (default "example-topic")
--group string       Consumer group ID (default "example-group")
--stream             Use streaming consumer mode (default true)
--auto-commit        Auto commit consumed messages (default true)
--max int            Maximum number of messages to consume, 0 for unlimited (default 0)
```

Example usage:

```bash
# Consume messages using streaming mode (default)
./consumer

# Consume messages using polling mode
./consumer --stream=false

# Consume a maximum of 10 messages and then exit
./consumer --max 10

# Consume from a specific topic with a specific group ID
./consumer --topics my-topic --group my-group-id
```

## Understanding the ConsumeStream Function

The `consumeStream` function in the consumer application demonstrates how to use the streaming consumer API:

1. It establishes a bidirectional streaming connection to the server
2. It continuously receives messages as they become available
3. It handles various error conditions gracefully
4. It can be interrupted by canceling its context
5. It automatically commits messages if auto-commit is enabled

This streaming approach is more efficient than polling for high-throughput scenarios as it:
- Reduces network overhead by maintaining a persistent connection
- Allows for real-time message processing
- Handles backpressure automatically

## Example Workflow

1. Start the server (not included in these examples)
2. Create a topic using the producer with the `--create` flag
3. Start the consumer to listen for messages
4. Run the producer to send messages to the topic
5. Observe the consumer receiving and processing the messages

This workflow simulates a typical publish-subscribe messaging pattern where producers and consumers operate independently. 