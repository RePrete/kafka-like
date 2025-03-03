package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Common errors
var (
	ErrTopicNotFound    = errors.New("topic not found")
	ErrNoMessage        = errors.New("no message available")
	ErrConsumerNotFound = errors.New("consumer not found")
	ErrConsumerExists   = errors.New("consumer already exists")
)

// Message represents a message in the queue
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Headers   []Header
}

// Header represents a key-value header for a message
type Header struct {
	Key   string
	Value []byte
}

// Producer interface mirroring Kafka producer
type Producer interface {
	// Produce sends a message to a topic
	Produce(ctx context.Context, msg *Message) error
	// Close cleans up resources
	Close() error
}

// Consumer interface mirroring Kafka consumer
type Consumer interface {
	// Subscribe subscribes to a list of topics
	Subscribe(topics []string) error
	// Consume polls for a message
	Consume(ctx context.Context, timeout time.Duration) (*Message, error)
	// ConsumeWithHandler continuously polls for messages and processes each with the provided handler function
	// If the handler returns nil, the message is automatically committed
	// This method runs until the context is canceled or an error occurs
	ConsumeWithHandler(ctx context.Context, timeout time.Duration, handler func(*Message) error) error
	// Commit commits a message offset
	Commit(msg *Message) error
	// Close cleans up resources
	Close() error
}

// Admin interface for managing topics
type Admin interface {
	// CreateTopic creates a new topic
	CreateTopic(ctx context.Context, name string, partitions int) error
	// DeleteTopic deletes a topic
	DeleteTopic(ctx context.Context, name string) error
	// ListTopics lists all topics
	ListTopics(ctx context.Context) ([]string, error)
	// Close cleans up resources
	Close() error
}

// QueueManager manages the message queue system
type QueueManager struct {
	topics         map[string]*topic
	producers      map[int]Producer
	consumers      map[int]Consumer
	nextProducerID int
	nextConsumerID int
}

// topic represents a message topic
type topic struct {
	name       string
	partitions []*partition
}

// partition represents a topic partition
type partition struct {
	id       int
	messages []*Message
	offset   int64
}

// consumerGroup manages a group of consumers
type consumerGroup struct {
	id            string
	topics        []string
	offsets       map[string]map[int]int64 // topic -> partition -> offset
	commitedMsgs  map[string]bool          // message unique ID -> committed
	lastHeartbeat time.Time
}

// NewQueueManager creates a new queue manager
func NewQueueManager() *QueueManager {
	return &QueueManager{
		topics:    make(map[string]*topic),
		producers: make(map[int]Producer),
		consumers: make(map[int]Consumer),
	}
}

// NewAdmin creates a new admin client
func (qm *QueueManager) NewAdmin() Admin {
	return &adminClient{qm: qm}
}

// NewProducer creates a new producer
func (qm *QueueManager) NewProducer() Producer {
	id := qm.nextProducerID
	qm.nextProducerID++

	producer := &producerClient{
		id: id,
		qm: qm,
	}

	qm.producers[producer.id] = producer
	return producer
}

// NewConsumer creates a new consumer with a group ID
func (qm *QueueManager) NewConsumer(groupID string) (Consumer, error) {
	id := qm.nextConsumerID
	qm.nextConsumerID++

	consumer := &consumerClient{
		id:      id,
		groupID: groupID,
		qm:      qm,
		group: &consumerGroup{
			id:            groupID,
			offsets:       make(map[string]map[int]int64),
			commitedMsgs:  make(map[string]bool),
			lastHeartbeat: time.Now(),
		},
	}

	qm.consumers[consumer.id] = consumer
	return consumer, nil
}

// Implementation of Admin interface
type adminClient struct {
	qm *QueueManager
}

func (a *adminClient) CreateTopic(ctx context.Context, name string, partitions int) error {
	// Check if topic already exists
	if _, exists := a.qm.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create partitions
	topicPartitions := make([]*partition, partitions)
	for i := 0; i < partitions; i++ {
		topicPartitions[i] = &partition{
			id:       i,
			messages: make([]*Message, 0),
			offset:   0,
		}
	}

	// Create topic
	a.qm.topics[name] = &topic{
		name:       name,
		partitions: topicPartitions,
	}

	return nil
}

func (a *adminClient) DeleteTopic(ctx context.Context, name string) error {
	// Check if topic exists
	if _, exists := a.qm.topics[name]; !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	// Delete topic
	delete(a.qm.topics, name)
	return nil
}

func (a *adminClient) ListTopics(ctx context.Context) ([]string, error) {
	topics := make([]string, 0, len(a.qm.topics))
	for name := range a.qm.topics {
		topics = append(topics, name)
	}
	return topics, nil
}

func (a *adminClient) Close() error {
	return nil
}

// Implementation of Producer interface
type producerClient struct {
	id int
	qm *QueueManager
}

func (p *producerClient) Produce(ctx context.Context, msg *Message) error {
	// Check if topic exists
	topic, exists := p.qm.topics[msg.Topic]
	if !exists {
		return fmt.Errorf("topic %s does not exist", msg.Topic)
	}

	// Check if partition exists
	if msg.Partition < 0 || msg.Partition >= len(topic.partitions) {
		return fmt.Errorf("invalid partition %d for topic %s", msg.Partition, msg.Topic)
	}

	partition := topic.partitions[msg.Partition]

	// Set message timestamp if not set
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Set message offset
	msg.Offset = partition.offset
	partition.offset++

	// Add message to partition
	partition.messages = append(partition.messages, msg)

	return nil
}

func (p *producerClient) Close() error {
	// Remove producer from queue manager
	delete(p.qm.producers, p.id)
	return nil
}

// Implementation of Consumer interface
type consumerClient struct {
	id      int
	groupID string
	qm      *QueueManager
	group   *consumerGroup
}

func (c *consumerClient) Subscribe(topics []string) error {
	// Check if topics exist
	for _, topicName := range topics {
		if _, exists := c.qm.topics[topicName]; !exists {
			return fmt.Errorf("topic %s does not exist", topicName)
		}
	}

	// Set topics for consumer group
	c.group.topics = topics

	// Initialize offsets for topics
	for _, topicName := range topics {
		topic := c.qm.topics[topicName]

		// Initialize topic offsets if not already initialized
		if _, exists := c.group.offsets[topicName]; !exists {
			c.group.offsets[topicName] = make(map[int]int64)
		}

		// Initialize partition offsets if not already initialized
		for _, partition := range topic.partitions {
			if _, exists := c.group.offsets[topicName][partition.id]; !exists {
				c.group.offsets[topicName][partition.id] = 0
			}
		}
	}

	return nil
}

func (c *consumerClient) Consume(ctx context.Context, timeout time.Duration) (*Message, error) {
	// Update heartbeat
	c.group.lastHeartbeat = time.Now()

	// Try to get a message from each topic and partition
	for _, topicName := range c.group.topics {
		topic, exists := c.qm.topics[topicName]
		if !exists {
			continue
		}

		for _, partition := range topic.partitions {
			// Get current offset for this partition
			offset, exists := c.group.offsets[topicName][partition.id]
			if !exists {
				continue
			}

			// Check if there are messages available at this offset
			if offset < partition.offset && offset >= 0 && int(offset) < len(partition.messages) {
				msg := partition.messages[int(offset)]

				// Check if message has already been committed
				msgID := generateMessageID(msg)
				if c.group.commitedMsgs[msgID] {
					// Message already committed, move to next offset
					c.group.offsets[topicName][partition.id]++
					continue
				}

				return msg, nil
			}
		}
	}

	// No messages available
	return nil, fmt.Errorf("no message available")
}

func (c *consumerClient) ConsumeWithHandler(ctx context.Context, timeout time.Duration, handler func(*Message) error) error {
	// Run until the context is canceled
	for {
		// Check if context is done before attempting to consume
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue processing
		}

		// Consume a message with a shorter timeout to allow for context cancellation checks
		consumeCtx, cancel := context.WithTimeout(ctx, timeout)
		msg, err := c.Consume(consumeCtx, timeout)
		cancel()

		if err != nil {
			// If no message is available, just continue the loop
			if err == ErrNoMessage {
				// Small sleep to prevent CPU spinning when no messages are available
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
			// For other errors, check if context was canceled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}

		// Process the message with the handler
		if err := handler(msg); err != nil {
			return err
		}

		// If handler was successful, commit the message
		if err := c.Commit(msg); err != nil {
			return err
		}
	}
}

func (c *consumerClient) Commit(msg *Message) error {
	// Check if topic exists
	if _, exists := c.group.offsets[msg.Topic]; !exists {
		return fmt.Errorf("topic %s not subscribed", msg.Topic)
	}

	// Check if partition exists
	if _, exists := c.group.offsets[msg.Topic][msg.Partition]; !exists {
		return fmt.Errorf("partition %d not subscribed for topic %s", msg.Partition, msg.Topic)
	}

	// Mark message as committed
	msgID := generateMessageID(msg)
	c.group.commitedMsgs[msgID] = true

	// Update offset if this is the current message
	currentOffset := c.group.offsets[msg.Topic][msg.Partition]
	if currentOffset == msg.Offset {
		c.group.offsets[msg.Topic][msg.Partition]++
	}

	return nil
}

func (c *consumerClient) Close() error {
	// Remove consumer from queue manager
	delete(c.qm.consumers, c.id)
	return nil
}

// Helper function to generate a unique ID for a message
func generateMessageID(msg *Message) string {
	return msg.Topic + "-" + string(msg.Partition) + "-" + string(msg.Offset)
}

// Example usage
func Example() {
	// Create queue manager
	qm := NewQueueManager()

	// Create admin client
	admin := qm.NewAdmin()

	// Create topic
	ctx := context.Background()
	admin.CreateTopic(ctx, "test-topic", 3)

	// Create producer
	producer := qm.NewProducer()

	// Produce initial message
	producer.Produce(ctx, &Message{
		Topic:     "test-topic",
		Partition: 0,
		Key:       []byte("key"),
		Value:     []byte("Hello, World!"),
	})

	// Create consumer
	consumer, _ := qm.NewConsumer("test-group")

	// Subscribe to topic
	consumer.Subscribe([]string{"test-topic"})

	// Example 1: Traditional consume and commit
	fmt.Println("Example 1: Traditional consume and commit")
	msg, err := consumer.Consume(ctx, 5*time.Second)
	if err != nil {
		fmt.Printf("Error consuming message: %v\n", err)
	} else {
		fmt.Printf("Consumed message: %s\n", string(msg.Value))
		consumer.Commit(msg)
	}

	// Example 2: Using ConsumeWithHandler for a single message
	fmt.Println("\nExample 2: Using ConsumeWithHandler for a single message")
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = consumer.ConsumeWithHandler(timeoutCtx, 1*time.Second, func(msg *Message) error {
		fmt.Printf("Processed single message: %s\n", string(msg.Value))
		return nil
	})
	cancel()
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		fmt.Printf("Error in single message handler: %v\n", err)
	}

	// Example 3: Using ConsumeWithHandler for continuous processing
	fmt.Println("\nExample 3: Using ConsumeWithHandler for continuous processing")

	// Create a context that can be canceled
	continuousCtx, stopConsumer := context.WithCancel(ctx)

	// Create a channel to signal when the consumer is done
	done := make(chan struct{})

	// Create a channel to track processed messages
	processed := make(chan string, 10)

	// Start a goroutine for continuous message processing
	go func() {
		defer close(done)

		err := consumer.ConsumeWithHandler(continuousCtx, 1*time.Second, func(msg *Message) error {
			messageContent := string(msg.Value)
			fmt.Printf("Processing message: %s\n", messageContent)

			// Simulate some processing work
			time.Sleep(100 * time.Millisecond)

			// Send to processed channel for tracking
			select {
			case processed <- messageContent:
				// Successfully sent to channel
			default:
				// Channel buffer full, just continue
			}

			return nil
		})

		if err != nil {
			if err == context.Canceled {
				fmt.Println("Consumer was gracefully stopped")
			} else {
				fmt.Printf("Consumer error: %v\n", err)
			}
		}
	}()

	// Start a producer in a separate goroutine
	go func() {
		// Produce messages at regular intervals
		for i := 0; i < 10; i++ {
			// Check if consumer has been stopped
			select {
			case <-continuousCtx.Done():
				return
			default:
				// Continue producing
			}

			time.Sleep(300 * time.Millisecond)

			message := fmt.Sprintf("Message %d", i)
			err := producer.Produce(ctx, &Message{
				Topic:     "test-topic",
				Partition: i % 3, // Distribute across partitions
				Key:       []byte(fmt.Sprintf("key-%d", i)),
				Value:     []byte(message),
			})

			if err != nil {
				fmt.Printf("Error producing message: %v\n", err)
			} else {
				fmt.Printf("Produced: %s\n", message)
			}
		}
	}()

	// Monitor processed messages
	go func() {
		count := 0
		for message := range processed {
			count++
			fmt.Printf("Successfully processed: %s (total: %d)\n", message, count)
		}
	}()

	// Let the consumer run for a while
	fmt.Println("Letting consumer run for 5 seconds...")
	time.Sleep(5 * time.Second)

	// Stop the continuous consumer
	fmt.Println("Stopping consumer...")
	stopConsumer()

	// Wait for consumer to finish
	<-done
	close(processed)

	fmt.Println("Consumer stopped, cleaning up...")

	// Clean up
	producer.Close()
	consumer.Close()
	admin.Close()

	fmt.Println("Example completed")
}
