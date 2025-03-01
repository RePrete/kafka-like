# Kafka-like Message Queue with gRPC

A simple Kafka-like message queue system with a gRPC interface.

## Features

- Topic management (create, delete, list)
- Message production and consumption
- Consumer groups
- Streaming consumption
- In-memory storage (for simplicity)

## Prerequisites

- Go 1.24 or later
- Protocol Buffers compiler (protoc)
- Go plugins for Protocol Buffers

## Installation

1. Clone the repository:

```bash
git clone https://github.com/RePrete/kafka-like.git
cd kafka-like
```

2. Generate gRPC code from proto files:

```bash
make proto
```

3. Build the server and examples:

```bash
make all
```

## Usage

### Starting the Server

```bash
make run
# or
bin/kafkalike-server
```

The server will start on port 50051 by default. You can specify a different port with the `-port` flag:

```bash
bin/kafkalike-server -port 8080
```

### Using the gRPC Client Example

The repository includes a simple gRPC client example that demonstrates how to use the server:
