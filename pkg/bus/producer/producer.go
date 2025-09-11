// Package produce implements a bufstream producer
package producer

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
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultClientID = "go-bus-producer"
	ModeAsync       = "async"
	ModeSync        = "sync"
)

// Producer is an example producer to a given topic using given Protobuf message type.
//
// A Producer takes a Kafka client and a topic, and sends one of two types of data:
//
//   - A Protobuf message of the given type.
//   - Invalid data that could not be parsed as any Protobuf message.
type Producer[M proto.Message] struct {
	clientID             string
	client               *kgo.Client
	serializer           serde.Serializer
	topic                string
	topicConfig          []string
	topicPartitions      int
	replicationFactor    int
	ensureTopic          bool
	clientConfig         kafka.Config
	logger               *slog.Logger
	defaultAsyncCallback func(r *kgo.Record, err error)

	saslUsername string
	saslPassword string

	// Close the producer and flush any async writes.
	// We can safely call this concurrently and multiple times, as it uses a sync.Once to ensure the client is only flushed and closed once.
	Close func()
}

// New returns a new Producer.
//
// Always use this constructor to construct Producers.
func New[M proto.Message](
	ctx context.Context,
	logger *slog.Logger,
	bootstrapServers []string,
	topic string,
	options ...ProducerOption[M],
) (*Producer[M], error) {
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

	producer := &Producer[M]{
		topic:             topic,
		clientID:          clientID,
		serializer:        bus.NewSerializer[M](),
		topicPartitions:   1,
		replicationFactor: 1,
	}
	if logger != nil {
		producer.logger = logger.With("component", "bus-producer", "topic", topic)
	}

	for _, option := range options {
		option(producer)
	}

	producer.clientConfig = kafka.Config{
		BootstrapServers:  bootstrapServers,
		ClientID:          producer.clientID,
		Topic:             topic,
		TopicConfig:       producer.topicConfig,
		TopicPartitions:   producer.topicPartitions,
		ReplicationFactor: producer.replicationFactor,
		SASLUsername:      producer.saslUsername,
		SASLPassword:      producer.saslPassword,
	}

	// Initialize the Kafka client if not already set.
	if producer.client == nil {
		// Default Kafka client configuration.
		client, err := kafka.NewKafkaClient(producer.clientConfig, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}
		producer.client = client
	}

	// Ensure the topic exists if requested.
	if producer.ensureTopic {
		if err := kafka.EnsureTopic(ctx, producer.client, producer.clientConfig); err != nil {
			return nil, fmt.Errorf("failed to ensure Kafka topic: %w", err)
		}
	}

	producer.defaultAsyncCallback = func(r *kgo.Record, err error) {
		if err != nil {
			producer.log(ctx, slog.LevelError, "failed to async produce record", "key", string(r.Key), "err", err)
		}
	}

	producer.Close = sync.OnceFunc(func() {
		producer.close()
	})

	return producer, nil
}

// ProducerOption is an option when constructing a new Producer.
//
// All parameters except options are required. ProducerOptions allow
// for optional parameters.
type ProducerOption[M proto.Message] func(*Producer[M])

// WithTopicConfig allows setting topic configuration options for the producer.
func WithTopicConfig[M proto.Message](config ...string) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicConfig = config
	}
}

// WithTopicPartitions allows setting the number of partitions for the topic.
func WithTopicPartitions[M proto.Message](partitions int) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicPartitions = partitions
	}
}

// WithReplicationFactor allows setting the replication factor for the topic.
func WithReplicationFactor[M proto.Message](replicationFactor int) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.replicationFactor = replicationFactor
	}
}

// WithRetentionBytes allows setting the retention bytes for the topic.
func WithRetentionBytes[M proto.Message](bytes int) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("retention.bytes=%d", bytes))
	}
}

// WithRetentionTime allows setting the retention time for the topic (to millisecond precision).
func WithRetentionTime[M proto.Message](duration time.Duration) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("retention.ms=%d", int64(duration/time.Millisecond)))
	}
}

// WithMaxMessageBytes allows setting the maximum message size for the topic.
// Default is 1MB (1048576 bytes).
func WithMaxMessageBytes[M proto.Message](bytes int) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicConfig = append(p.topicConfig, fmt.Sprintf("max.message.bytes=%d", bytes))
	}
}

// WithInfiniteRetention allows setting the topic to have infinite retention time.
// By default Topics have a retention time of 7 days and infinite retention bytes.
func WithInfiniteRetention[M proto.Message]() ProducerOption[M] {
	return func(p *Producer[M]) {
		p.topicConfig = append(p.topicConfig, "retention.ms=-1")
	}
}

// WithClient allows initializing the producer with the specified Kafka client.
// This overrides the provided topic configuration and partitions.
func WithClient[M proto.Message](client *kgo.Client) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.client = client
	}
}

// WithHostOverride returns a new Producer Option that overrides the default broker host
// This allows us to connect to the Kafka broker using a different hostname than what the broker advertises.
func WithHostOverride[M proto.Message](host string) ProducerOption[M] {
	return func(producer *Producer[M]) {
		if host != "" {
			producer.clientID += ";host_override=" + host
		}
	}
}

// WithEnsureTopic allows setting whether to ensure the topic exists before producing messages.
// If true, the producer will attempt to create the topic if it does not exist.
func WithEnsureTopic[M proto.Message](ensure bool) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.ensureTopic = ensure
	}
}

// WithCredentials sets the SASL username and password for the producer.
func WithCredentials[M proto.Message](username, password string) ProducerOption[M] {
	return func(p *Producer[M]) {
		p.saslUsername = username
		p.saslPassword = password
	}
}

// ProduceAsync serializes the given Protobuf messages, and asynchronously
// sends it to the Producer's topic with the given partition key.
// This should return immediately, and the message will be sent in the background.
// Errors returned from this method are only related to serialization issues.
func (p *Producer[M]) ProduceAsync(ctx context.Context, key string, message M, cb func(r *kgo.Record, err error)) error {
	start := time.Now()
	res := "ok"
	defer func() {
		produceDuration.WithLabelValues(p.topic, ModeAsync, res).Observe(time.Since(start).Seconds())
		messagesProduced.WithLabelValues(p.topic, ModeAsync, res).Inc()
	}()

	payload, err := p.serializer.Serialize(p.topic, message)
	if err != nil {
		res = "error"
		return err
	}

	if cb == nil {
		cb = p.defaultAsyncCallback
	}

	rec := &kgo.Record{
		Key:   []byte(key),
		Value: payload,
		Topic: p.topic,
	}

	p.client.Produce(ctx, rec, cb)

	return nil
}

// ProduceSync serializes the given Protobuf messages, and synchronously
// sends it to the Producer's topic with the given partition key.
// This will block until the message is sent or an error occurs.
func (p *Producer[M]) ProduceSync(ctx context.Context, key string, message M) error {
	start := time.Now()
	res := "ok"
	defer func() {
		produceDuration.WithLabelValues(p.topic, ModeSync, res).Observe(time.Since(start).Seconds())
		messagesProduced.WithLabelValues(p.topic, ModeSync, res).Inc()
	}()

	payload, err := p.serializer.Serialize(p.topic, message)
	if err != nil {
		res = "error"
		return err
	}

	if err := p.client.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(key),
		Value: payload,
		Topic: p.topic,
	}).FirstErr(); err != nil {
		res = "error"
		return fmt.Errorf("failed to synchronously produce record with key %q: %w", key, err)
	}
	return nil
}

func (p *Producer[M]) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.log(ctx, slog.LevelInfo, "closing producer, waiting up to 5 seconds to flush async writes")

	err := p.client.Flush(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.log(context.Background(), slog.LevelWarn, "producer flush timed out, closing client without waiting for all messages to be sent")
		} else {
			p.log(context.Background(), slog.LevelError, "failed to flush producer", "err", err)
		}
	} else {
		p.logger.Info("producer flushed successfully on close")
	}
	p.client.Close()
}

func (p *Producer[M]) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if p.logger != nil {
		p.logger.Log(ctx, level, msg, args...)
	}
}
