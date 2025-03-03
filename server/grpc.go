package server

import (
	"context"
	"fmt"
	"time"

	pb "github.com/RePrete/kafka-like/proto/gen/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the QueueService gRPC service
type GRPCServer struct {
	pb.UnimplementedQueueServiceServer
	qm             *QueueManager
	consumers      map[string]Consumer
	nextConsumerID int
	consumersByID  map[string]string // consumerID -> groupID mapping
}

// NewGRPCServer creates a new gRPC server with the given queue manager
func NewGRPCServer(qm *QueueManager) *GRPCServer {
	return &GRPCServer{
		qm:            qm,
		consumers:     make(map[string]Consumer),
		consumersByID: make(map[string]string),
	}
}

// CreateTopic implements the CreateTopic RPC
func (s *GRPCServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	admin := s.qm.NewAdmin()
	defer admin.Close()

	err := admin.CreateTopic(ctx, req.Name, int(req.Partitions))
	if err != nil {
		return &pb.CreateTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.CreateTopicResponse{
		Success: true,
	}, nil
}

// DeleteTopic implements the DeleteTopic RPC
func (s *GRPCServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	admin := s.qm.NewAdmin()
	defer admin.Close()

	err := admin.DeleteTopic(ctx, req.Name)
	if err != nil {
		return &pb.DeleteTopicResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.DeleteTopicResponse{
		Success: true,
	}, nil
}

// ListTopics implements the ListTopics RPC
func (s *GRPCServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	admin := s.qm.NewAdmin()
	defer admin.Close()

	topics, err := admin.ListTopics(ctx)
	if err != nil {
		return &pb.ListTopicsResponse{
			Error: err.Error(),
		}, nil
	}

	return &pb.ListTopicsResponse{
		Topics: topics,
	}, nil
}

// Produce implements the Produce RPC
func (s *GRPCServer) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	producer := s.qm.NewProducer()
	defer producer.Close()

	msg := &Message{
		Key:       req.Message.Key,
		Value:     req.Message.Value,
		Topic:     req.Message.Topic,
		Partition: int(req.Message.Partition),
		Timestamp: time.Now(),
	}

	// Convert headers if any
	if len(req.Message.Headers) > 0 {
		msg.Headers = make([]Header, len(req.Message.Headers))
		for i, h := range req.Message.Headers {
			msg.Headers[i] = Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
	}

	err := producer.Produce(ctx, msg)
	if err != nil {
		return &pb.ProduceResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.ProduceResponse{
		Success: true,
	}, nil
}

// Subscribe implements the Subscribe RPC
func (s *GRPCServer) Subscribe(ctx context.Context, req *pb.SubscribeRequest) (*pb.SubscribeResponse, error) {
	consumer, err := s.qm.NewConsumer(req.GroupId)
	if err != nil {
		return &pb.SubscribeResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	err = consumer.Subscribe(req.Topics)
	if err != nil {
		consumer.Close()
		return &pb.SubscribeResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Generate a unique consumer ID
	consumerID := fmt.Sprintf("%s-%d", req.GroupId, s.nextConsumerID)
	s.nextConsumerID++

	// Store the consumer for later use
	s.consumers[consumerID] = consumer
	s.consumersByID[consumerID] = req.GroupId

	return &pb.SubscribeResponse{
		Success:    true,
		ConsumerId: consumerID,
	}, nil
}

// Consume implements the Consume RPC
func (s *GRPCServer) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	// Find the consumer
	consumer, exists := s.consumers[req.ConsumerId]
	if !exists {
		return &pb.ConsumeResponse{
			Error: "consumer not found",
		}, nil
	}

	// Try to consume a message
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	msg, err := consumer.Consume(ctx, timeout)
	if err != nil {
		return &pb.ConsumeResponse{
			Error: err.Error(),
		}, nil
	}

	// Convert message to protobuf format
	pbMsg := &pb.QueueMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: int32(msg.Partition),
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp.UnixNano(),
		Headers:   make([]*pb.MessageHeader, len(msg.Headers)),
	}

	for i, header := range msg.Headers {
		pbMsg.Headers[i] = &pb.MessageHeader{
			Key:   header.Key,
			Value: header.Value,
		}
	}

	return &pb.ConsumeResponse{
		Message: pbMsg,
	}, nil
}

// Commit implements the Commit RPC
func (s *GRPCServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	// Find the consumer
	consumer, exists := s.consumers[req.ConsumerId]
	if !exists {
		return &pb.CommitResponse{
			Success: false,
			Error:   "consumer not found",
		}, nil
	}

	// Convert protobuf message to internal format
	msg := &Message{
		Key:       req.Message.Key,
		Value:     req.Message.Value,
		Topic:     req.Message.Topic,
		Partition: int(req.Message.Partition),
		Offset:    req.Message.Offset,
		Timestamp: time.Unix(0, req.Message.Timestamp),
		Headers:   make([]Header, len(req.Message.Headers)),
	}

	for i, header := range req.Message.Headers {
		msg.Headers[i] = Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}

	// Commit the message
	err := consumer.Commit(msg)
	if err != nil {
		return &pb.CommitResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.CommitResponse{
		Success: true,
	}, nil
}

// ConsumeStream implements the streaming consumer RPC
func (s *GRPCServer) ConsumeStream(req *pb.ConsumeStreamRequest, stream pb.QueueService_ConsumeStreamServer) error {
	consumer, exists := s.consumers[req.ConsumerId]
	if !exists {
		return status.Errorf(codes.NotFound, "consumer not found: %s", req.ConsumerId)
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	ctx := stream.Context()

	// Use ConsumeWithHandler to continuously process messages
	return consumer.ConsumeWithHandler(ctx, timeout, func(msg *Message) error {
		// Convert message to protobuf format
		pbMsg := &pb.QueueMessage{
			Key:       msg.Key,
			Value:     msg.Value,
			Topic:     msg.Topic,
			Partition: int32(msg.Partition),
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp.UnixMilli(),
		}

		// Convert headers if any
		if len(msg.Headers) > 0 {
			pbMsg.Headers = make([]*pb.MessageHeader, len(msg.Headers))
			for i, h := range msg.Headers {
				pbMsg.Headers[i] = &pb.MessageHeader{
					Key:   h.Key,
					Value: h.Value,
				}
			}
		}

		// Send the message to the client
		if err := stream.Send(&pb.ConsumeResponse{
			Message: pbMsg,
		}); err != nil {
			return err
		}

		return nil
	})
}

// Close implements the Close method for the GRPCServer
func (s *GRPCServer) Close() error {
	// Close all consumers
	for _, consumer := range s.consumers {
		consumer.Close()
	}

	// Clear maps
	s.consumers = make(map[string]Consumer)
	s.consumersByID = make(map[string]string)

	return nil
}

// Helper function to convert internal Message to protobuf QueueMessage
func messageToProto(msg *Message) *pb.QueueMessage {
	pbMsg := &pb.QueueMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: int32(msg.Partition),
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp.UnixMilli(),
	}

	if len(msg.Headers) > 0 {
		pbMsg.Headers = make([]*pb.MessageHeader, len(msg.Headers))
		for i, h := range msg.Headers {
			pbMsg.Headers[i] = &pb.MessageHeader{
				Key:   h.Key,
				Value: h.Value,
			}
		}
	}

	return pbMsg
}

// Helper function to convert protobuf QueueMessage to internal Message
func protoToMessage(pbMsg *pb.QueueMessage) *Message {
	msg := &Message{
		Key:       pbMsg.Key,
		Value:     pbMsg.Value,
		Topic:     pbMsg.Topic,
		Partition: int(pbMsg.Partition),
		Offset:    pbMsg.Offset,
		Timestamp: time.UnixMilli(pbMsg.Timestamp),
	}

	if len(pbMsg.Headers) > 0 {
		msg.Headers = make([]Header, len(pbMsg.Headers))
		for i, h := range pbMsg.Headers {
			msg.Headers[i] = Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
	}

	return msg
}
