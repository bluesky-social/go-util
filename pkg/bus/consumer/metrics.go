package consumer

import (
	"github.com/bluesky-social/go-util/pkg/bus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var messagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: bus.MetricsNamespace,
	Name:      "messages_consumed_total",
	Help:      "Total number of messages consumed from Kafka.",
}, []string{"topic", "status"})

var consumeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: bus.MetricsNamespace,
	Name:      "consume_duration_seconds",
	Help:      "Duration of consuming messages from Kafka.",
	Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
}, []string{"topic", "status"})
