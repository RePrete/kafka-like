FROM alpine:latest

# Install required packages
RUN apk add --no-cache protoc go git

# Set up Go environment
ENV GOPATH=/go
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# Install protoc plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Verify installation
RUN which protoc-gen-go && which protoc-gen-go-grpc

WORKDIR /workspace