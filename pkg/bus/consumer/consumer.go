// Package consume implements a bufstream consumer
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus"
	"github.com/bluesky-social/go-util/pkg/bus/kafka"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type Offset string

const (
	DefaultClientID        = "go-bus-consumer"
	OffsetStart     Offset = "start"
	OffsetEnd       Offset = "end"
)

type tp struct {
	t string
	p int32
}

type partitionConsumer[M proto.Message] struct {
	con *Consumer[M]

	cl        *kgo.Client
	topic     string
	partition int32
	logger    *slog.Logger

	deserializer         serde.Deserializer
	messageHandler       func(context.Context, M) error
	malformedDataHandler func([]byte, error) error

	quit chan struct{}
	done chan struct{}
	recs chan kgo.FetchTopicPartition
}

// Consumer is an example consumer of a given topic using given Protobuf message type.
//
// A Consume takes a Kafka client and a topic, and expects to recieve Protobuf messages
// of the given type. Upon every received message, a handler is invoked. If malformed
// data is recieved (data that can not be deserialized into the given Protobuf message type),
// a malformed data handler is invoked.
type Consumer[M proto.Message] struct {
	bootstrapServers     []string
	clientID             string
	client               *kgo.Client
	deserializer         serde.Deserializer
	topic                string
	consumerGroup        string
	offset               *Offset
	messageHandler       func(context.Context, M) error
	malformedDataHandler func([]byte, error) error
	handlerConcurrency   int64
	batchSize            int
	logger               *slog.Logger

	consumers *xsync.MapOf[tp, *partitionConsumer[M]]

	Consume func(ctx context.Context) error
	inOrder bool

	quit  chan struct{}
	Close func()

	dlqTopic        string
	dlqProducerOpts []producer.ProducerOption[M]
	dlqProducer     *producer.Producer[M]

	saslUsername string
	saslPassword string
}

// New returns a new Consumer.
//
// Always use this constructor to construct Consumers.
func New[M proto.Message](
	logger *slog.Logger,
	bootstrapServers []string,
	topic string,
	consumerGroup string,
	options ...ConsumerOption[M],
) (*Consumer[M], error) {
	ctx := context.Background()

	if len(bootstrapServers) == 0 {
		return nil, errors.New("at least one bootstrap server must be provided")
	}

	// By default, append the first bootstrap server's host to the client ID.
	// This should allow us to connect to the Kafka broker using a different hostname
	// than what the broker advertises, which is useful when connecting to a Kafka cluster
	// from outside of the k8s cluster
	parts := strings.Split(bootstrapServers[0], ":")
	firstBootstrapHost := parts[0]
	var clientID string

	clientHostname, err := os.Hostname()
	if err != nil {
		clientID = fmt.Sprintf("%s;host_override=%s", DefaultClientID, firstBootstrapHost)
	} else {
		clientID = fmt.Sprintf("%s;host_override=%s", clientHostname, firstBootstrapHost)
	}

	consumer := &Consumer[M]{
		bootstrapServers: bootstrapServers,
		clientID:         clientID,
		topic:            topic,
		consumerGroup:    consumerGroup,
		deserializer:     bus.NewDeserializer[M](),
		consumers:        xsync.NewMapOf[tp, *partitionConsumer[M]](),
		batchSize:        10_000,
		quit:             make(chan struct{}),
	}
	consumer.messageHandler = consumer.defaultMessageHandler
	consumer.malformedDataHandler = consumer.defaultMalformedDataHandler

	// Default to in-order consumption.
	consumer.Consume = consumer.consumeInOrder
	consumer.inOrder = true

	consumer.Close = sync.OnceFunc(func() {
		consumer.close()
	})

	if logger != nil {
		consumer.logger = logger.With("component", "bus-consumer", "topic", topic)
	}

	for _, option := range options {
		option(consumer)
	}

	// Initialize the Kafka client if not already set.
	if consumer.client == nil {
		conOpts := kafka.DefaultConsumerOpts()
		conOpts.AutoCommitMarks = true
		if consumer.offset != nil {
			switch *consumer.offset {
			case OffsetStart:
				conOpts.Offset = kafka.OffsetStart
			case OffsetEnd:
				conOpts.Offset = kafka.OffsetEnd
			default:
			}
		} else {
			// Default to consuming from the end of the topic if no offset is specified.
			conOpts.Offset = kafka.OffsetEnd
		}

		if consumer.inOrder {
			// If in-order consumption is requested, set up callbacks and block rebalancing.
			conOpts.BlockRebalanceOnPoll = true
			conOpts.OnPartitionsAssigned = consumer.assigned
			conOpts.OnPartitionsRevoked = consumer.revoked
			conOpts.OnPartitionsLost = consumer.lost
		}

		client, err := kafka.NewKafkaClient(kafka.Config{
			BootstrapServers: bootstrapServers,
			Group:            consumer.consumerGroup,
			ClientID:         consumer.clientID,
			Topic:            topic,
			SASLUsername:     consumer.saslUsername,
			SASLPassword:     consumer.saslPassword,
		}, conOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}
		consumer.client = client
	}

	if consumer.dlqTopic != "" {
		// Always create the DLQ topic if it doesn't exist yet
		defaultOpts := []producer.ProducerOption[M]{
			producer.WithEnsureTopic[M](true),
		}

		// Read the topic config for the primary topic and copy relevant settings to the DLQ topic
		// This ensures the DLQ topic has similar configuration to the primary topic
		topicConfig, numPartitions, err := kafka.GetTopicSettings(ctx, consumer.client, consumer.topic)
		if err != nil {
			consumer.log(ctx, slog.LevelError, "failed to get topic config", "error", err)
		}
		defaultOpts = append(defaultOpts, producer.WithTopicConfig[M](topicConfig...))
		defaultOpts = append(defaultOpts, producer.WithTopicPartitions[M](numPartitions))

		// Prepend the defaults to the producer options passed in, allowing
		// the caller to override any of the default settings.
		consumer.dlqProducerOpts = append(defaultOpts, consumer.dlqProducerOpts...)

		// Create a new producer for the dead letter queue
		producer, err := producer.New(ctx, consumer.logger, consumer.bootstrapServers, consumer.dlqTopic, consumer.dlqProducerOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create dead letter queue producer: %w", err)
		}
		consumer.dlqProducer = producer
	}

	return consumer, nil
}

// ConsumerOption is an option when constructing a new Consumer.
//
// All parameters except options are required. ConsumerOptions allow
// for optional parameters.
type ConsumerOption[M proto.Message] func(*Consumer[M])

// WithClient initializes the consumer with the specified Kafka client.
// This overrides the provided consumergroup and topic.
func WithClient(client *kgo.Client) ConsumerOption[proto.Message] {
	return func(consumer *Consumer[proto.Message]) {
		consumer.client = client
	}
}

// WithOffset returns a new ConsumerOption that overrides the default offset from which to start consuming messages.
// This only applies if the consumer is not joining an existing consumer group, in which case the consumer will resume
// from the last committed offset for the group.
func WithOffset[M proto.Message](offset Offset) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		consumer.offset = &offset
	}
}

// WithMessageHandler returns a new ConsumerOption that overrides the default
// handler of received messages.
//
// The default handler uses the provided logger to log incoming messages.
func WithMessageHandler[M proto.Message](messageHandler func(context.Context, M) error) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		consumer.messageHandler = messageHandler
	}
}

// WithHostOverride returns a new Consumer Option that overrides the default broker host
// This allows us to connect to the Kafka broker using a different hostname than what the broker advertises.
func WithHostOverride[M proto.Message](host string) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		if host != "" {
			consumer.clientID += ";host_override=" + host
		}
	}
}

// WithInOrderConsumption returns a new Consumer Option that sets the consumer to consume messages strictly in order.
// Topics with multiple partitions will be consumed concurrently, but messages within each partition will be processed in the order they were received.
func WithInOrderConsumption[M proto.Message]() ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		consumer.Consume = consumer.consumeInOrder
		consumer.inOrder = true
	}
}

// WithBatchSize returns a new Consumer Option that sets the batch size for consuming messages in order.
func WithBatchSize[M proto.Message](batchSize int) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		if batchSize <= 0 {
			consumer.log(context.Background(), slog.LevelWarn,
				"invalid batch size, using default value of 10_000")
			batchSize = 10_000
		}
		consumer.batchSize = batchSize
	}
}

// WithOutOfOrderConsumption returns a new Consumer Option that sets the consumer to consume messages potentially out of order.
// This allows for concurrent processing of records within a single topic partition, which can increase throughput at the cost of potentially out-of-order processing.
func WithOutOfOrderConsumption[M proto.Message](concurrency int64) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		if concurrency <= 0 {
			consumer.log(context.Background(), slog.LevelWarn,
				"invalid handler concurrency, using default value of 1")
			concurrency = 1
		}
		consumer.Consume = consumer.consumeOutOfOrder
		consumer.handlerConcurrency = concurrency
		consumer.inOrder = false
	}
}

// WithDeadLetterQueue returns a new Consumer Option that creates a default topic and producer for handling dead letter messages.
func WithDeadLetterQueue[M proto.Message](producerOpts ...producer.ProducerOption[M]) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		// Don't create a DLQ if the input topic already ends in "-dlq".
		if strings.HasSuffix(consumer.topic, "-dlq") {
			return
		}

		// Create a default dead letter queue topic name
		consumer.dlqTopic = fmt.Sprintf("%s-%s-dlq", consumer.topic, consumer.consumerGroup)
		consumer.dlqProducerOpts = append(consumer.dlqProducerOpts, producerOpts...)
	}
}

// WithCredentials sets the SASL username and password for the consumer.
func WithCredentials[M proto.Message](username, password string) ConsumerOption[M] {
	return func(c *Consumer[M]) {
		c.saslUsername = username
		c.saslPassword = password
	}
}

// WithMalformedDataHandler returns a new Consumer Option that overrides the default
// handler of malformed received data.
//
// The default handler uses the provided logger to log the error returned on malformed data.
func WithMalformedDataHandler[M proto.Message](
	malformedDataHandler func([]byte, error) error,
) ConsumerOption[M] {
	return func(consumer *Consumer[M]) {
		consumer.malformedDataHandler = malformedDataHandler
	}
}

var ErrClientClosed = errors.New("consumer client is closed")

// consumeInOrder consumes records from the topic in order.
// It creates a partition consumer for each topic partition and ensures all records are processed in the order they were received.
// This consumer blocks rebalancing while processing records, so batch sizes should be sized such that consumers are
// finished with a batch quickly if you intend to scale consumers up and down dynamically.
// See https://github.com/twmb/franz-go/blob/a849b8be17b75bb74aa954cb29c9b4934d8b8ead/examples/goroutine_per_partition_consuming/autocommit_marks/main.go#L144
func (c *Consumer[M]) consumeInOrder(ctx context.Context) error {
	for {
		fetches := c.client.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			c.log(ctx, slog.LevelInfo, "client is closed, stopping consumer")
			return ErrClientClosed
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			c.client.AllowRebalance()
			return fmt.Errorf("failed to fetch records: %v", errs)
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			tp := tp{t: partition.Topic, p: partition.Partition}
			pc, exists := c.consumers.Load(tp)
			if !exists {
				c.log(ctx, slog.LevelDebug, "partition consumer not found", "topic", partition.Topic, "partition", partition.Partition)
				return
			}

			// Exit if the consumer is closed.
			select {
			case <-c.quit:
				return
				// Since we block rebalance on poll, the partition consumer should already exist.
			case pc.recs <- partition:
			}
		})
		c.client.AllowRebalance()
	}
}

// consumeOutOfOrder consumes records from the topic in a potentially out-of-order manner.
// it allows for concurrent processing of records within a single topic partition
// this provides higher throughput at the cost of potentially out-of-order processing
// it is useful for topics where the order of messages is not critical
func (c *Consumer[M]) consumeOutOfOrder(ctx context.Context) error {
	handleRecord := func(record *kgo.Record) error {
		start := time.Now()
		res := "ok"
		defer func() {
			consumeDuration.WithLabelValues(c.topic, res).Observe(time.Since(start).Seconds())
			messagesConsumed.WithLabelValues(c.topic, res).Inc()
		}()

		data, err := c.deserializer.Deserialize(record.Topic, record.Value)
		if err != nil {
			if err := c.malformedDataHandler(record.Value, err); err != nil {
				res = "error_malformed"
				return fmt.Errorf("malformed data handler failed: %w", err)
			}
		}
		message, ok := data.(M)
		if !ok {
			res = "error_malformed"
			if err := c.malformedDataHandler(
				record.Value,
				fmt.Errorf("received unexpected message type: %T", data),
			); err != nil {
				return fmt.Errorf("malformed data handler failed: %w", err)
			}
		}

		// Make sure we commit the record after processing it.
		defer func() {
			c.client.MarkCommitRecords(record)
		}()

		if err := c.messageHandler(ctx, message); err != nil {
			res = "error_handler"
			return fmt.Errorf("message handler failed: %w", err)
		}

		return nil
	}

	wg := sync.WaitGroup{}

	work := make(chan *kgo.Record, c.handlerConcurrency)
	quit := make(chan struct{})
	consumerExit := sync.OnceFunc(func() {
		c.log(ctx, slog.LevelInfo, "consumer exiting, closing quit channel")
		close(quit)
	})

	for range c.handlerConcurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range work {
				if err := handleRecord(record); err != nil {
					c.log(ctx, slog.LevelError, "failed to handle record", "error", err, "record", record)
					consumerExit()
					return
				}
			}
		}()
	}

	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			c.log(ctx, slog.LevelInfo, "client is closed, stopping consumer")
			wg.Wait()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("failed to fetch records: %v", errs)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			select {
			case <-ctx.Done():
				c.log(ctx, slog.LevelInfo, "context canceled, stopping consumer")
				wg.Wait()
				return nil
			case <-quit:
				c.log(ctx, slog.LevelInfo, "quit signal received, stopping consumer")
				wg.Wait()
				return nil
			default:
				record := iter.Next()
				work <- record
			}
		}
	}
}

func (c *Consumer[M]) close() {
	// Stop the consumer from trying to assign new work
	close(c.quit)

	// Kill all partition consumers and wait for them to finish existing work
	tps := make(map[string][]int32)
	c.consumers.Range(func(tp tp, pc *partitionConsumer[M]) bool {
		tps[tp.t] = append(tps[tp.t], tp.p)
		return true
	})
	c.killConsumers(tps)

	// Commit any remaining offsets
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.client.CommitMarkedOffsets(ctx); err != nil {
		c.log(ctx, slog.LevelError, "failed to commit marked offsets", "error", err)
	} else {
		c.log(ctx, slog.LevelInfo, "committed marked offsets")
	}

	// If we have a DLQ producer, shut it down gracefully
	if c.dlqProducer != nil {
		c.log(ctx, slog.LevelInfo, "shutting down DLQ producer")
		c.dlqProducer.Close()
	}

	c.client.Close()
}

func (c *Consumer[M]) defaultMessageHandler(ctx context.Context, message M) error {
	c.log(ctx, slog.LevelInfo, "received message", "message", message)
	return nil
}

func (c *Consumer[M]) defaultMalformedDataHandler(_ []byte, err error) error {
	c.log(context.Background(), slog.LevelError, "malformed data received", "error", err)
	return nil
}

func (c *Consumer[M]) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if c.logger != nil {
		c.logger.Log(ctx, level, msg, args...)
	}
}

func (pc *partitionConsumer[M]) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if pc.logger != nil {
		pc.logger.Log(ctx, level, msg, args...)
	}
}

func (pc *partitionConsumer[M]) consume() {
	defer close(pc.done)

	ctx := context.Background()

	for {
		select {
		case <-pc.quit:
			return
		case p := <-pc.recs:
			for _, record := range p.Records {
				if err := pc.handleRecord(record); err != nil {
					pc.log(ctx, slog.LevelError, "failed to handle record", "error", err, "record_key", string(record.Key), "record_offset", record.Offset)
				}
			}
		}
	}

}

func (pc *partitionConsumer[M]) handleRecord(record *kgo.Record) error {
	ctx := context.Background()

	start := time.Now()
	res := "ok"
	defer func() {
		consumeDuration.WithLabelValues(pc.topic, res).Observe(time.Since(start).Seconds())
		messagesConsumed.WithLabelValues(pc.topic, res).Inc()
	}()

	data, err := pc.deserializer.Deserialize(record.Topic, record.Value)
	if err != nil {
		if err := pc.malformedDataHandler(record.Value, err); err != nil {
			res = "error_malformed"
			return fmt.Errorf("malformed data handler failed: %w", err)
		}
	}
	message, ok := data.(M)
	if !ok {
		res = "error_malformed"
		if err := pc.malformedDataHandler(
			record.Value,
			fmt.Errorf("received unexpected message type: %T", data),
		); err != nil {
			return fmt.Errorf("malformed data handler failed: %w", err)
		}
	}

	// Make sure we commit the record after processing it.
	defer func() {
		pc.cl.MarkCommitRecords(record)
	}()

	if err := pc.messageHandler(ctx, message); err != nil {
		res = "error_handler"
		if pc.con.dlqProducer != nil {
			pc.log(ctx, slog.LevelError, "failed to handle record, forwarding to dead letter queue",
				"error", err,
				"record_key", string(record.Key),
				"record_offset", record.Offset,
				"dlq_topic", pc.con.dlqTopic,
			)
			// Send the record to the dead letter queue
			if err := pc.con.dlqProducer.ProduceAsync(context.Background(), string(record.Key), message, nil); err != nil {
				pc.log(ctx, slog.LevelError, "failed to produce to DLQ", "error", err)
			}
		} else {
			pc.log(ctx, slog.LevelError, "failed to handle record, dropping on the ground and will not retry", "error", err, "record_key", record.Key, "record_offset", record.Offset)
		}

		// Return nil since we've already handled the error
		return nil
	}

	return nil
}

func (c *Consumer[M]) assigned(ctx context.Context, kClient *kgo.Client, assigned map[string][]int32) {
	parts := make([]string, 0, len(assigned))
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			parts = append(parts, fmt.Sprintf("%s-%d", topic, partition))
			topicPartition := tp{t: topic, p: partition}
			if _, exists := c.consumers.Load(topicPartition); exists {
				c.log(ctx, slog.LevelDebug, "partition consumer already exists", "topic", topic, "partition", partition)
				continue
			}

			pc := &partitionConsumer[M]{
				con:                  c,
				cl:                   kClient,
				topic:                topic,
				partition:            partition,
				deserializer:         c.deserializer,
				messageHandler:       c.messageHandler,
				malformedDataHandler: c.malformedDataHandler,
				quit:                 make(chan struct{}),
				done:                 make(chan struct{}),
				recs:                 make(chan kgo.FetchTopicPartition, 5),
			}

			if c.logger != nil {
				pc.logger = c.logger.With("topic", topic, "partition", partition)
			}

			c.consumers.Store(topicPartition, pc)
			go pc.consume()
		}
	}
	c.log(ctx, slog.LevelInfo, "assigned partitions", "partitions", strings.Join(parts, ", "))
}

func (c *Consumer[M]) killConsumers(lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for topic, partitions := range lost {
		for _, partition := range partitions {
			topicPartition := tp{t: topic, p: partition}
			pc, exists := c.consumers.Load(topicPartition)
			if !exists {
				c.log(context.Background(), slog.LevelDebug, "partition consumer not found", "topic", topic, "partition", partition)
				continue
			}

			wg.Add(1)
			go func(pc *partitionConsumer[M]) {
				defer wg.Done()
				pc.log(context.Background(), slog.LevelInfo, "closing partition consumer")
				close(pc.quit)
				<-pc.done
				c.log(context.Background(), slog.LevelInfo, "partition consumer closed", "topic", pc.topic, "partition", pc.partition)
			}(pc)
			c.consumers.Delete(topicPartition)
		}
	}
}

func (c *Consumer[M]) revoked(ctx context.Context, kClient *kgo.Client, revoked map[string][]int32) {
	c.log(ctx, slog.LevelInfo, "revoked partitions", "partitions", c.formatParts(revoked))
	c.killConsumers(revoked)
	if err := kClient.CommitMarkedOffsets(ctx); err != nil {
		c.log(ctx, slog.LevelError, "failed to commit marked offsets", "error", err)
	}
}

func (c *Consumer[M]) lost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.log(ctx, slog.LevelInfo, "lost partitions", "partitions", c.formatParts(lost))
	c.killConsumers(lost)
	// We can't commit lost partitions, as they are no longer assigned to us.
}

func (c *Consumer[M]) formatParts(parts map[string][]int32) string {
	partStrs := make([]string, 0, len(parts))
	for topic, partitions := range parts {
		for _, partition := range partitions {
			partStrs = append(partStrs, fmt.Sprintf("%s-%d", topic, partition))
		}
	}
	return strings.Join(partStrs, ", ")
}
