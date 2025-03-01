package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	mu             sync.RWMutex
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
	mu         sync.RWMutex
}

// partition represents a topic partition
type partition struct {
	id       int
	messages []*Message
	offset   int64
	mu       sync.RWMutex
}

// consumerGroup manages a group of consumers
type consumerGroup struct {
	id            string
	topics        []string
	offsets       map[string]map[int]int64 // topic -> partition -> offset
	mu            sync.RWMutex
	commitedMsgs  map[string]bool // message unique ID -> committed
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
	qm.mu.Lock()
	defer qm.mu.Unlock()

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
	qm.mu.Lock()
	defer qm.mu.Unlock()

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
	a.qm.mu.Lock()
	defer a.qm.mu.Unlock()

	if _, exists := a.qm.topics[name]; exists {
		return nil // Topic already exists
	}

	newTopic := &topic{
		name:       name,
		partitions: make([]*partition, partitions),
	}

	for i := 0; i < partitions; i++ {
		newTopic.partitions[i] = &partition{
			id:       i,
			messages: make([]*Message, 0),
			offset:   0,
		}
	}

	a.qm.topics[name] = newTopic
	return nil
}

func (a *adminClient) DeleteTopic(ctx context.Context, name string) error {
	a.qm.mu.Lock()
	defer a.qm.mu.Unlock()

	if _, exists := a.qm.topics[name]; !exists {
		return ErrTopicNotFound
	}

	delete(a.qm.topics, name)
	return nil
}

func (a *adminClient) ListTopics(ctx context.Context) ([]string, error) {
	a.qm.mu.RLock()
	defer a.qm.mu.RUnlock()

	topics := make([]string, 0, len(a.qm.topics))
	for topicName := range a.qm.topics {
		topics = append(topics, topicName)
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
	p.qm.mu.RLock()
	topic, exists := p.qm.topics[msg.Topic]
	p.qm.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	topic.mu.RLock()
	if msg.Partition >= len(topic.partitions) || msg.Partition < 0 {
		msg.Partition = 0 // Default to first partition if invalid
	}
	partition := topic.partitions[msg.Partition]
	topic.mu.RUnlock()

	partition.mu.Lock()
	defer partition.mu.Unlock()

	msg.Offset = partition.offset
	msg.Timestamp = time.Now()

	partition.messages = append(partition.messages, msg)
	partition.offset++

	return nil
}

func (p *producerClient) Close() error {
	p.qm.mu.Lock()
	defer p.qm.mu.Unlock()

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
	c.qm.mu.RLock()
	defer c.qm.mu.RUnlock()

	c.group.mu.Lock()
	defer c.group.mu.Unlock()

	// Verify all topics exist
	for _, topicName := range topics {
		if _, exists := c.qm.topics[topicName]; !exists {
			return ErrTopicNotFound
		}

		// Initialize offset tracking for this topic
		if _, exists := c.group.offsets[topicName]; !exists {
			c.group.offsets[topicName] = make(map[int]int64)

			// Initialize offsets to 0 for all partitions
			topic := c.qm.topics[topicName]
			for i := 0; i < len(topic.partitions); i++ {
				c.group.offsets[topicName][i] = 0
			}
		}
	}

	c.group.topics = topics
	c.group.lastHeartbeat = time.Now()

	return nil
}

func (c *consumerClient) Consume(ctx context.Context, timeout time.Duration) (*Message, error) {
	c.group.mu.Lock()
	c.group.lastHeartbeat = time.Now()
	topics := c.group.topics
	c.group.mu.Unlock()

	if len(topics) == 0 {
		return nil, errors.New("no topics subscribed")
	}

	// Set a deadline based on the timeout
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, topicName := range topics {
			c.qm.mu.RLock()
			topic, exists := c.qm.topics[topicName]
			c.qm.mu.RUnlock()

			if !exists {
				continue
			}

			topic.mu.RLock()
			partitions := topic.partitions
			topic.mu.RUnlock()

			for _, partition := range partitions {
				partition.mu.RLock()

				c.group.mu.RLock()
				offset, exists := c.group.offsets[topicName][partition.id]
				c.group.mu.RUnlock()

				if !exists {
					partition.mu.RUnlock()
					continue
				}

				// Check if there are messages available at or after the current offset
				if int64(len(partition.messages)) > offset {
					msg := partition.messages[offset]
					partition.mu.RUnlock()
					return msg, nil
				}

				partition.mu.RUnlock()
			}
		}

		// No messages found, wait a bit before trying again
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue trying
		}
	}

	return nil, ErrNoMessage
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
	c.group.mu.Lock()
	defer c.group.mu.Unlock()

	// Update the offset for this topic and partition
	if _, exists := c.group.offsets[msg.Topic]; !exists {
		c.group.offsets[msg.Topic] = make(map[int]int64)
	}

	// Set the offset to the next message
	c.group.offsets[msg.Topic][msg.Partition] = msg.Offset + 1

	// Mark this message as committed
	msgID := generateMessageID(msg)
	c.group.commitedMsgs[msgID] = true

	return nil
}

func (c *consumerClient) Close() error {
	c.qm.mu.Lock()
	defer c.qm.mu.Unlock()

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
