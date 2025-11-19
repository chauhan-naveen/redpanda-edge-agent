package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	promServerStarted int32
	podName           = os.Getenv("POD_NAME")
	lastScrapeTime    atomic.Value  // stores time.Time of last successful scrape

	// Agent health metric: 1 = fully healthy, 0.5 = partially healthy, 0 = unhealthy
	agentUp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_up",
			Help: "1 if agent has healthy connectivity to both source and destination clusters, 0 otherwise.",
		},
		[]string{"pod"},
	)

	// Total number of brokers in source and destination clusters
	sourceBrokersTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_source_brokers_total",
			Help: "Number of brokers in the source cluster.",
		},
		[]string{"pod"},
	)
	destinationBrokersTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_destination_brokers_total",
			Help: "Number of brokers in the destination cluster.",
		},
		[]string{"pod"},
	)

	// Total number of topics in source and destination clusters
	sourceTopicsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_source_topics_total",
			Help: "Number of topics in the source cluster.",
		},
		[]string{"pod"},
	)
	destinationTopicsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_destination_topics_total",
			Help: "Number of topics in the destination cluster.",
		},
		[]string{"pod"},
	)

	// Total number of partitions in source and destination clusters
	sourcePartitionsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_source_partitions_total",
			Help: "Total number of partitions in the source cluster.",
		},
		[]string{"pod"},
	)
	destinationPartitionsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_destination_partitions_total",
			Help: "Total number of partitions in the destination cluster.",
		},
		[]string{"pod"},
	)

	// Number of partitions with uncommitted offsets
	uncommittedOffsetsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_uncommitted_offsets_total",
			Help: "Total number of uncommitted offsets observed.",
		},
		[]string{"pod"},
	)

	// Partition count per topic
	partitionsPerTopic = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_partitions_per_topic",
			Help: "Partition count for a topic.",
		},
		[]string{"pod", "cluster", "topic"},
	)

	// Ping latency metrics in milliseconds
	sourcePingMs = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_source_ping_ms",
			Help: "Ping latency to the source cluster (ms).",
		},
		[]string{"pod"},
	)
	destinationPingMs = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_destination_ping_ms",
			Help: "Ping latency to the destination cluster (ms).",
		},
		[]string{"pod"},
	)

	// Timestamp of last scrape
	lastScrapeUnix = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_metrics_last_scrape_unix",
			Help: "Unix timestamp of the last metrics scrape.",
		},
		[]string{"pod"},
	)
	lastScrapeAgeSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_metrics_last_scrape_age_seconds",
			Help: "Number of seconds since the last metrics scrape was performed.",
		},
		[]string{"pod"},
	)

	// Aggregate lag metrics across all partitions
	totalLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_total_lag",
			Help: "Total consumer lag across all partitions.",
		},
		[]string{"pod"},
	)
	maxLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_max_lag",
			Help: "Maximum partition lag observed.",
		},
		[]string{"pod"},
	)

	// Committed offsets per partition
	committedOffset = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_committed_offset",
			Help: "Committed offset per partition.",
		},
		[]string{"pod", "cluster", "topic", "partition"},
	)

	// Total bytes consumed and produced
	consumerBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_consumer_bytes_total",
			Help: "Total bytes consumed from source.",
		},
		[]string{"pod"},
	)
	producerBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_producer_bytes_total",
			Help: "Total bytes produced to destination.",
		},
		[]string{"pod"},
	)

	// Record counts for fetching and producing
	RecordsFetched = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_records_fetched_total",
			Help: "Total number of records fetched from the source cluster",
		},
		[]string{"pod", "direction"},
	)
	RecordsSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_records_sent_total",
			Help: "Total number of records successfully sent to the destination cluster",
		},
		[]string{"pod", "direction"},
	)

	// Error counters
	FetchErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_fetch_errors_total",
			Help: "Total number of fetch errors encountered",
		},
		[]string{"pod", "direction"},
	)
	ProduceErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_produce_errors_total",
			Help: "Total number of produce errors encountered",
		},
		[]string{"pod", "direction"},
	)
	CommitErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redpanda_edge_agent_commit_errors_total",
			Help: "Total number of commit errors encountered",
		},
		[]string{"pod", "direction"},
	)

	// Histogram for backoff durations in seconds
	BackoffDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redpanda_edge_agent_backoff_duration_seconds",
			Help:    "Duration of backoffs due to errors (in seconds)",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{"pod", "direction"},
	)

	// Per-partition lag metric
	partitionLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redpanda_edge_agent_partition_lag",
			Help: "Lag per partition (difference between latest offset and committed offset).",
		},
		[]string{"pod", "cluster", "topic", "partition"},
	)
)

func StartMetricsServer() {
	if !atomic.CompareAndSwapInt32(&promServerStarted, 0, 1) {
		return
	}
	port := 8080
	if p := os.Getenv("REDPANDA_METRICS_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	} else if p := os.Getenv("DD_AGENT_EXPORTER_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	addr := ":" + strconv.Itoa(port)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
			log.Errorf("Metrics server error: %v", err)
		}
	}()
	log.Infof("Prometheus metrics server listening on %s/metrics", addr)
}

func metricsCollectorLoop() {
	// Wait up to 30s for source & destination clients to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			log.Warn("Metrics collector timed out waiting for clients")
			return
		default:
			if source.client != nil && destination.client != nil {
				goto start
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

start:
  // Collect metrics every 15 seconds
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		collectOnce(context.Background())
	}
}

// Collect metrics for a single scrape
func collectOnce(ctx context.Context) {
	now := time.Now()
	// Record scrape timestamp
	lastScrapeUnix.WithLabelValues(podName).Set(float64(now.Unix()))

	// Calculate age since previous scrape
	if prev, ok := lastScrapeTime.Load().(time.Time); ok {
		age := now.Sub(prev).Seconds()
		lastScrapeAgeSeconds.WithLabelValues(podName).Set(age)
	} else {
		// First scrape: set 0
		lastScrapeAgeSeconds.WithLabelValues(podName).Set(0)
	}
	lastScrapeTime.Store(now)

	srcUp := pingCluster(ctx, source.client, sourcePingMs, "source")
	dstUp := pingCluster(ctx, destination.client, destinationPingMs, "destination")

	if srcUp && dstUp {
		agentUp.WithLabelValues(podName).Set(1)
	} else if srcUp || dstUp {
		agentUp.WithLabelValues(podName).Set(0.5)
	} else {
		agentUp.WithLabelValues(podName).Set(0)
	}

	partitionsPerTopic.Reset()

	if source.adm != nil {
		if brs, err := source.adm.ListBrokers(ctx); err == nil {
			sourceBrokersTotal.WithLabelValues(podName).Set(float64(len(brs)))
		} else {
			log.Warnf("Failed to list brokers for source: %v", err)
		}

		if tmap, err := source.adm.ListTopics(ctx); err == nil {
			sourceTopicsTotal.WithLabelValues(podName).Set(float64(len(tmap)))
			partitions := 0
			for topic, details := range tmap {
				partitionsPerTopic.WithLabelValues(podName, "source", topic).Set(float64(len(details.Partitions)))
				partitions += len(details.Partitions)
			}
			sourcePartitionsTotal.WithLabelValues(podName).Set(float64(partitions))
		} else {
			log.Warnf("Failed to list topics for source: %v", err)
		}
	}

	if destination.adm != nil {
		if brs, err := destination.adm.ListBrokers(ctx); err == nil {
			destinationBrokersTotal.WithLabelValues(podName).Set(float64(len(brs)))
		} else {
			log.Warnf("Failed to list brokers for destination: %v", err)
		}

		if tmap, err := destination.adm.ListTopics(ctx); err == nil {
			destinationTopicsTotal.WithLabelValues(podName).Set(float64(len(tmap)))
			partitions := 0
			for topic, details := range tmap {
				partitionsPerTopic.WithLabelValues(podName, "destination", topic).Set(float64(len(details.Partitions)))
				partitions += len(details.Partitions)
			}
			destinationPartitionsTotal.WithLabelValues(podName).Set(float64(partitions))
		} else {
			log.Warnf("Failed to list topics for destination: %v", err)
		}
	}

	total := 0
	maxv := 0
	uncommittedOffsetsTotal.WithLabelValues(podName).Set(0)
	committedOffset.Reset()

	collectLag(ctx, "source", source, &total, &maxv)
	collectLag(ctx, "destination", destination, &total, &maxv)

	totalLag.WithLabelValues(podName).Set(float64(total))
	maxLag.WithLabelValues(podName).Set(float64(maxv))
}

// Ping a cluster and record latency
func pingCluster(ctx context.Context, cl interface{ Ping(context.Context) error }, gauge *prometheus.GaugeVec, name string) bool {
	if cl == nil {
		gauge.WithLabelValues(podName).Set(0)
		return false
	}
	start := time.Now()
	err := cl.Ping(ctx)
	elapsed := time.Since(start)
	gauge.WithLabelValues(podName).Set(float64(elapsed.Milliseconds()))
	if err != nil {
		log.Errorf("Ping failed for %s cluster: %v", name, err)
		return false
	}
	return true
}

// Collect per-partition lag and committed offsets
func collectLag(ctx context.Context, clusterName string, cluster Redpanda, total *int, maxv *int) {
	if cluster.client == nil || cluster.adm == nil {
		return
	}
  // Get all uncommitted offsets from the cluster client
	offsets := cluster.client.UncommittedOffsets()
	if offsets == nil {
		return
	}

	validTopics := make(map[string]bool)
	for _, topic := range cluster.topics {
		validTopics[topic.consumeFrom()] = true
	}

	partitionCountWithLag := 0

	for topic, partitions := range offsets {
		if !validTopics[topic] {
			continue
		}

		// List end offsets from the cluster
		endOffsetsMap, err := cluster.adm.ListEndOffsets(ctx, topic)
		if err != nil {
			log.Warnf("Failed to list end offsets for %s topic %s: %v", clusterName, topic, err)
			continue
		}

		endOffsets, ok := endOffsetsMap[topic]
		if !ok {
			log.Warnf("No end offsets found for topic %s", topic)
			continue
		}

		// Iterate over partitions and calculate lag
		for partition, off := range partitions {
			endOffset, ok := endOffsets[partition]
			if !ok {
				log.Warnf("No end offset for %s topic %s partition %d", clusterName, topic, partition)
				continue
			}
			if endOffset.Err != nil {
				log.Warnf("Error retrieving end offset for %s topic %s partition %d: %v",
					clusterName, topic, partition, endOffset.Err)
				continue
			}

			// Compute lag in messages
			lag := int(endOffset.Offset - off.Offset)
			if lag > 0 {
				*total += lag
				partitionCountWithLag++
				if lag > *maxv {
					*maxv = lag
				}
			}

			// Record committed offset for this partition
			committedOffset.WithLabelValues(
				podName,
				clusterName,
				topic,
				strconv.Itoa(int(partition)),
			).Set(float64(off.Offset))

			// Record per-partition lag metric
			partitionLag.WithLabelValues(
				podName,
				clusterName,
				topic,
				strconv.Itoa(int(partition)),
			).Set(float64(lag))
		}
	}

	// Update total uncommitted partitions
	uncommittedOffsetsTotal.WithLabelValues(podName).Set(float64(partitionCountWithLag))
}

func init() {
	go StartMetricsServer()
	go metricsCollectorLoop()
}
