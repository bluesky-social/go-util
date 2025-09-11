package producer

import (
	"github.com/bluesky-social/go-util/pkg/bus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var messagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: bus.MetricsNamespace,
	Name:      "messages_produced_total",
	Help:      "Total number of messages produced to Kafka.",
}, []string{"topic", "mode", "status"})

var produceDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: bus.MetricsNamespace,
	Name:      "produce_duration_seconds",
	Help:      "Duration of producing messages to Kafka.",
	Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
}, []string{"topic", "mode", "status"})
