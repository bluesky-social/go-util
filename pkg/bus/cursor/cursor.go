// cursor implements a simple Kafka-backed cursor mechanism for saving and loading
// the last processed message in a stream. It ensures that messages are saved and loaded
// in order, and provides a way to resume processing from the last saved cursor.
// This is useful for applications that need to maintain a cursor state across restarts or failures
// when you only want to use Kafka as a storage backend.
package cursor

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
	"github.com/rs/xid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultClientID   = "go-bus-cursor"
	replicationFactor = 3
	partitions        = 1
)

type Cursor[M proto.Message] struct {
	producerClient *kgo.Client
	consumerClient *kgo.Client
	serializer     serde.Serializer
	deserializer   serde.Deserializer
	topic          string
	logger         *slog.Logger
	group     string
	closeOnce sync.Once

	saslUsername string
	saslPassword string
}

func New[M proto.Message](
	ctx context.Context,
	bootstrapServers []string,
	topic string,
	options ...CursorOption[M],
) (*Cursor[M], error) {
	if len(bootstrapServers) == 0 {
		return nil, errors.New("at least one bootstrap server must be provided")
	}

	consumerGroup := fmt.Sprintf("cursor-%s", xid.New().String())

	cursor := &Cursor[M]{
		topic:        topic,
		serializer:   bus.NewSerializer[M](),
		deserializer: bus.NewDeserializer[M](),
		logger:       slog.Default().With("component", "bus-cursor", "topic", topic),
		group:        consumerGroup,
	}

	var clientID string

	parts := strings.Split(bootstrapServers[0], ":")
	firstBootstrapHost := parts[0]

	clientHostname, err := os.Hostname()
	if err != nil {
		clientID = fmt.Sprintf("%s;host_override=%s", DefaultClientID, firstBootstrapHost)
	} else {
		clientID = fmt.Sprintf("%s;host_override=%s", clientHostname, firstBootstrapHost)
	}

	for _, option := range options {
		option(cursor)
	}

	cfg := kafka.Config{
		BootstrapServers:  bootstrapServers,
		ClientID:          clientID,
		Topic:             topic,
		Group:             cursor.group,
		TopicPartitions:   partitions,
		ReplicationFactor: replicationFactor,
		TopicConfig: []string{
			"retention.bytes=10000000", // 10MB
		},
		SASLUsername: cursor.saslUsername,
		SASLPassword: cursor.saslPassword,
	}

	producerClient, err := kafka.NewKafkaClient(cfg, nil)
	if err != nil {
		return nil, err
	}
	cursor.producerClient = producerClient

	// Ensure the topic exists
	if err := kafka.EnsureTopic(ctx, producerClient, cfg); err != nil {
		producerClient.Close()
		return nil, fmt.Errorf("failed to ensure topic %s exists: %w", topic, err)
	}

	// We only care about the most recent messages, so we set the offset to the end.
	conOpts := kafka.DefaultConsumerOpts()
	conOpts.Offset = kafka.OffsetEnd

	consumerClient, err := kafka.NewKafkaClient(cfg, conOpts)
	if err != nil {
		producerClient.Close()
		return nil, fmt.Errorf("failed to create consumer client: %w", err)
	}
	cursor.consumerClient = consumerClient

	return cursor, nil
}

// CursorOption is an option when constructing a new Cursor.
//
// All parameters except options are required. ConsumerOptions allow
// for optional parameters.
type CursorOption[M proto.Message] func(*Cursor[M])

// WithCredentials sets the SASL username and password for the cursor clients.
func WithCredentials[M proto.Message](username, password string) CursorOption[M] {
	return func(c *Cursor[M]) {
		c.saslUsername = username
		c.saslPassword = password
	}
}

func (c *Cursor[M]) Save(ctx context.Context, message M) error {
	payload, err := c.serializer.Serialize(c.topic, message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	if err := c.producerClient.ProduceSync(ctx, &kgo.Record{
		Value: payload,
		Topic: c.topic,
	}).FirstErr(); err != nil {
		return fmt.Errorf("failed to synchronously produce record: %w", err)
	}

	return nil
}

func (c *Cursor[M]) Load(ctx context.Context, isFinal func(M) bool) (M, error) {
	cursorLoadContext, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var m M
	found := false
	connected := false

	// Make sure we've got the latest cursor before we return it.
	// When a cursor caller shuts down, they may save the cursor on exit so if we start consuming immediately,
	// we might miss the last message they saved.
	for {
		fetches := c.consumerClient.PollFetches(cursorLoadContext)
		if fetches.IsClientClosed() {
			return m, errors.New("cursor consumer client is closed")
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// If we timed out and successfully connected but never saw any records,
			// the cursor topic is empty — that's not an error, there's just no cursor saved yet.
			// If we timed out without ever getting a successful fetch, the cluster is unreachable.
			if errors.Is(errs[0].Err, context.DeadlineExceeded) && connected && !found {
				return m, nil
			}
			return m, fmt.Errorf("failed to fetch records: %v", errs)
		}

		// A successful fetch (even with zero records) means the cluster is reachable.
		connected = true

		// If we're not waiting for a specific cursor state and the topic is empty, return immediately
		// rather than blocking for the full timeout duration.
		if isFinal == nil && len(fetches.Records()) == 0 {
			return m, nil
		}

		// Keep the last record and return it
		for _, record := range fetches.Records() {
			data, err := c.deserializer.Deserialize(record.Topic, record.Value)
			if err != nil {
				return m, fmt.Errorf("failed to deserialize record: %w", err)
			}

			message, ok := data.(M)
			if !ok {
				return m, fmt.Errorf("received unexpected message type: %T", data)
			}
			m = message
			found = true
		}

		// If we have a final check function, use it to determine if the previously saved cursor is the final one.
		if found && isFinal != nil && isFinal(m) {
			return m, nil
		}

		// If isFinal is nil and we found a record, return it immediately.
		if found && isFinal == nil {
			return m, nil
		}
	}
}

func (c *Cursor[M]) Close() {
	c.closeOnce.Do(func() {
		c.producerClient.Close()

		deleteCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Delete consumer offsets to clean up after ourselves.
		if err := kafka.DeleteOffsets(deleteCtx, c.consumerClient, c.group, c.topic, []int32{0}); err != nil {
			c.logger.Error("failed to delete offsets", "err", err)
		}

		c.consumerClient.Close()
	})
}
