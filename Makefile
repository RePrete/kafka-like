.PHONY: build clean proto

# Build the server binary
build:
	go build -o bin/kafkalike-server cmd/server/main.go

run-server:
	go build -o bin/kafkalike-server cmd/server/main.go && bin/kafkalike-server

run-consumer:
	go run examples/cmd/consumer/main.go -addr localhost:50051 -topic test-topic -group test-group

run-producer:
	go run examples/cmd/producer/main.go -addr localhost:50051 -topic test-topic -count 10 -interval 1s

# Generate gRPC code from proto files
proto: all-docker
	mkdir -p proto/gen
	docker run --rm \
		-v $(shell pwd):/workspace \
		kafkalike-proto \
		/bin/sh -c "cd /workspace && protoc -I=. --go_out=./proto/gen --go_opt=paths=source_relative --go-grpc_out=./proto/gen --go-grpc_opt=paths=source_relative ./proto/queue.proto"

# Build all
all: proto build build-example build-grpc-example

# Build Docker image for protoc
all-docker:
	docker build -t kafkalike-proto -f scripts/proto/Dockerfile .

# Clean built binaries
clean:
	rm -rf bin/
	rm -rf proto/gen/