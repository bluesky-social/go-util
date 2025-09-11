package bus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/bluesky-social/go-util/pkg/bus/testproto"
	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	testKafkaBootstrapServers = []string{"localhost:9092"}
)

func TestE2E(t *testing.T) {
	ctx := t.Context()
	logger := slog.Default()

	testTopic := "e2e_test_topic_" + xid.New().String()
	consumerGroup := "e2e_test_consumer_group_" + xid.New().String()

	busProducer, err := producer.New(ctx, logger, testKafkaBootstrapServers, testTopic,
		producer.WithEnsureTopic[*testproto.FirehoseEvent](true),
		producer.WithReplicationFactor[*testproto.FirehoseEvent](1),
		producer.WithTopicPartitions[*testproto.FirehoseEvent](20),
		producer.WithRetentionTime[*testproto.FirehoseEvent](time.Minute*5),
	)
	require.NoError(t, err, "failed to create Kafka producer")

	// Generate 10 actors
	actors := make([]string, 10)
	for i := range 10 {
		actors[i] = fmt.Sprintf("did:example:actor%d", i)
	}

	totalProduced := atomic.Int64{}

	// Generate 100 posts for each actor
	recordsProduced := map[string][]*testproto.FirehoseEvent{}
	recLk := sync.Mutex{}

	wg := sync.WaitGroup{}
	for _, actor := range actors {
		wg.Add(1)
		go func(actor string) {
			defer wg.Done()
			for j := range 100 {
				textContent := fmt.Sprintf("Post %d from %s", j, actor)
				evt, _, err := createPostEvent(actor, textContent)
				require.NoError(t, err, "failed to create post event")
				err = busProducer.ProduceAsync(ctx, actor, evt, nil)
				require.NoError(t, err, "failed to produce event")
				recLk.Lock()
				recordsProduced[actor] = append(recordsProduced[actor], evt)
				recLk.Unlock()
				totalProduced.Add(1)
			}
		}(actor)
	}

	wg.Wait()

	// Close and flush the producer to ensure all events are sent
	busProducer.Close()

	seenPerActor := xsync.NewMapOfPresized[string, int](len(actors))
	for _, actor := range actors {
		seenPerActor.Store(actor, 0)
	}

	totalSeen := atomic.Int64{}

	// Expect that we receive all events in order per actor
	handleEvent := func(ctx context.Context, event *testproto.FirehoseEvent) error {
		// Compare the record with the expected data
		idx, _ := seenPerActor.Load(event.Did)
		expectedEvt := recordsProduced[event.Did][idx]
		expectedRecord := expectedEvt.Commit.Record

		require.NotEmpty(t, event.Commit.Record, "record should not be empty")
		require.Equal(t, expectedRecord, event.Commit.Record,
			"record mismatch for actor %s at index %d", event.Did, idx)

		seenPerActor.Store(event.Did, idx+1)
		totalSeen.Add(1)
		return nil
	}

	busConsumer, err := consumer.New(logger, testKafkaBootstrapServers, testTopic, consumerGroup,
		consumer.WithOffset[*testproto.FirehoseEvent](consumer.OffsetStart),
		consumer.WithMessageHandler(handleEvent),
	)
	require.NoError(t, err, "failed to create Bus consumer")

	// Start a routine to close the consumer after all events are seen
	go func() {
		for {
			if totalSeen.Load() >= totalProduced.Load() {
				logger.Info("all events seen, closing consumer")
				busConsumer.Close()
				return
			}
			time.Sleep(100 * time.Millisecond) // Check every 100ms
		}
	}()

	// All messages should be consumed
	err = busConsumer.Consume(ctx)
	require.ErrorIs(t, err, consumer.ErrClientClosed, "expected consumer to close after processing all messages")

	// Verify that all events were seen
	require.Equal(t, totalProduced.Load(), totalSeen.Load(), "not all events were seen")
}

func createPostEvent(actor, textContent string) (*testproto.FirehoseEvent, string, error) {
	post := apibsky.FeedPost{
		Text: textContent,
	}

	postBytes, err := json.Marshal(post)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal post: %w", err)
	}

	rkey := syntax.NewTIDNow(0).String()

	evt := testproto.FirehoseEvent{
		Did:       actor,
		Timestamp: timestamppb.Now(),
		Kind:      testproto.EventKind_EVENT_KIND_COMMIT,
		Commit: &testproto.Commit{
			Collection: "app.bsky.feed.post",
			Rkey:       rkey,
			Record:     postBytes,
		},
	}

	return &evt, rkey, nil
}
