package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Config is all configuration we need to build a new Kafka Client.
type Config struct {
	// BootstrapServers are the bootstrap servers to call.
	BootstrapServers  []string
	RootCAPath        string
	Group             string
	ClientID          string
	Topic             string
	TopicConfig       []string
	TopicPartitions   int
	ReplicationFactor int
	SASLUsername      string
	SASLPassword      string
}

type Offset string

const (
	OffsetStart Offset = "start"
	OffsetEnd   Offset = "end"
)

type ConsumerOpts struct {
	Offset               Offset
	AutoCommitMarks      bool
	OnPartitionsAssigned func(context.Context, *kgo.Client, map[string][]int32)
	OnPartitionsRevoked  func(context.Context, *kgo.Client, map[string][]int32)
	OnPartitionsLost     func(context.Context, *kgo.Client, map[string][]int32)
	BlockRebalanceOnPoll bool
}

func DefaultConsumerOpts() *ConsumerOpts {
	return &ConsumerOpts{
		Offset:          OffsetStart,
		AutoCommitMarks: false,
	}
}

// NewKafkaClient returns a new franz-go Kafka Client for the given Config.
// If you provide conOpts, it will be used to configure the client as a consumer.
func NewKafkaClient(config Config, conOpts *ConsumerOpts) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.BootstrapServers...),
		kgo.ClientID(config.ClientID),
		kgo.ProducerBatchMaxBytes(20 << 20), // 20 MiB
	}

	if config.SASLUsername != "" && config.SASLPassword != "" {
		opts = append(opts,
			kgo.SASL(plain.Auth{
				User: config.SASLUsername,
				Pass: config.SASLPassword,
			}.AsMechanism()),
		)
	}

	if conOpts != nil {
		opts = append(opts,
			kgo.ConsumerGroup(config.Group),
			kgo.ConsumeTopics(config.Topic),
			kgo.FetchMaxWait(time.Second),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.RequireStableFetchOffsets(),
		)
		switch conOpts.Offset {
		case OffsetStart:
			// Start consuming from the beginning of the topic.
			opts = append(opts,
				kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
		case OffsetEnd:
			// Start consuming from the end of the topic.
			// If something goes wrong, we will reset the offset to the start.
			opts = append(opts,
				kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd().Relative(-1)),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
		default:
		}
		if conOpts.AutoCommitMarks {
			// Enable auto-commit of marks and block rebalance while a consumer is polling.
			// This is useful for ensuring that messages are not lost during rebalances.
			opts = append(opts,
				kgo.AutoCommitMarks(),
			)
		}
		if conOpts.OnPartitionsAssigned != nil {
			// Register a callback for when partitions are assigned to the consumer.
			opts = append(opts, kgo.OnPartitionsAssigned(conOpts.OnPartitionsAssigned))
		}
		if conOpts.OnPartitionsRevoked != nil {
			// Register a callback for when partitions are revoked from the consumer.
			opts = append(opts, kgo.OnPartitionsRevoked(conOpts.OnPartitionsRevoked))
		}
		if conOpts.OnPartitionsLost != nil {
			// Register a callback for when partitions are lost from the consumer.
			opts = append(opts, kgo.OnPartitionsLost(conOpts.OnPartitionsLost))
		}
		if conOpts.BlockRebalanceOnPoll {
			// Block rebalancing while the consumer is polling.
			// This is useful for ensuring that messages are not lost during rebalances.
			opts = append(opts, kgo.BlockRebalanceOnPoll())
		}
	}

	if config.RootCAPath != "" {
		dialerTLSConfig, err := buildDialerTLSConfig(config.RootCAPath)
		if err != nil {
			return nil, fmt.Errorf("build dial tls config: %w", err)
		}

		opts = append(opts, kgo.DialTLSConfig(dialerTLSConfig))
	}

	return kgo.NewClient(opts...)
}

func buildDialerTLSConfig(rootCAPath string) (*tls.Config, error) {
	pool := x509.NewCertPool()

	caCert, err := os.ReadFile(rootCAPath)
	if err != nil {
		return nil, err
	}

	if !pool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("parse CA cert failed")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}

	return tlsCfg, nil
}

// EnsureTopic ensures that the given topic exists in the Kafka cluster.
// If the topic does not exist, it will be created with the given configuration.
func EnsureTopic(ctx context.Context, client *kgo.Client, config Config) error {
	// Check if the topic already exists
	admClient := kadm.NewClient(client)
	resp, err := admClient.DescribeTopicConfigs(ctx, config.Topic)
	if err == nil {
		if len(resp) != 1 {
			return fmt.Errorf("expected one topic config, got %d", len(resp))
		}
		err = resp[0].Err
	}
	if err == nil {
		return nil // Topic already exists
	}

	// Create the topic if it does not exist
	configs := make(map[string]*string, len(config.TopicConfig))
	for _, cfg := range config.TopicConfig {
		k, v, _ := strings.Cut(cfg, "=")
		if v == "" {
			configs[k] = nil
		} else {
			configs[k] = &v
		}
	}

	topicPartitions := int32(1)
	if config.TopicPartitions > 0 {
		topicPartitions = int32(config.TopicPartitions)
	}

	replicationFactor := int16(1)
	if config.ReplicationFactor > 0 {
		replicationFactor = int16(config.ReplicationFactor)
	}

	creationResp, err := admClient.CreateTopic(ctx, topicPartitions, replicationFactor, configs, config.Topic)
	if err == nil {
		err = creationResp.Err
	}

	slog.Info("created bus topic", "resp", creationResp, "topic", config.Topic)

	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", config.Topic, err)
	}
	return nil
}

// GetTopicSettings retrieves the configuration settings for a given Kafka topic.
func GetTopicSettings(ctx context.Context, client *kgo.Client, topic string) ([]string, int, error) {
	admClient := kadm.NewClient(client)

	ltResp, err := admClient.ListTopics(ctx, topic)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list topics %s: %w", topic, err)
	}

	if ltResp.Error() != nil {
		return nil, 0, fmt.Errorf("failed to list topics %s: %w", topic, ltResp.Error())
	}

	t, ok := ltResp[topic]
	if !ok {
		return nil, 0, fmt.Errorf("topic %s does not exist", topic)
	}

	partitionCount := len(t.Partitions.Numbers())

	dtcResp, err := admClient.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to describe topic %s: %w", topic, err)
	}
	if len(dtcResp) == 0 {
		return nil, 0, fmt.Errorf("no configurations found for topic %s", topic)
	}

	configs := []string{}
	for _, cfg := range dtcResp[0].Configs {
		configs = append(configs, fmt.Sprintf("%s=%s", cfg.Key, cfg.MaybeValue()))
	}

	slog.Info("retrieved topic settings", "topic", topic, "settings", configs)

	return configs, partitionCount, nil
}

func DeleteOffsets(ctx context.Context, client *kgo.Client, consumerGroup string, topic string, partitions []int32) error {
	admClient := kadm.NewClient(client)
	topicSet := kadm.TopicsSet{}
	topicSet.Add(topic, partitions...)

	resp, err := admClient.DeleteOffsets(ctx, consumerGroup, topicSet)
	if err != nil {
		return fmt.Errorf("failed to delete offsets for topic %s: %w", topic, err)
	}
	if resp.Error() != nil {
		return fmt.Errorf("failed to delete offsets for topic %s: %w", topic, resp.Error())
	}

	return nil
}
