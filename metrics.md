**Metric by Metric** break down:

1. Whether the metric is **per-pod** or **aggregated across all pods**
2. Its **unit/type** (ms, bytes, count, gauge, etc.)
3. Its **significance / what it tells you**

All metrics here come from the `metrics.go` Prometheus setup.

---

### **1. `redpanda_edge_agent_up`**

* **Type:** Gauge (1 = healthy, 0 = down, 0.5 = partial)
* **Unit:** Boolean-like (1/0)
* **Scope:** Per pod (labeled with `pod`)
* **Significance:** Indicates whether the agent can successfully reach **both source and destination clusters**.

  * 1 → connectivity healthy
  * 0.5 → only one cluster reachable
  * 0 → neither cluster reachable

---

### **2. `redpanda_edge_agent_source_brokers_total`** & **3. `redpanda_edge_agent_destination_brokers_total`**

* **Type:** Gauge
* **Unit:** Count (number of brokers)
* **Scope:** Per pod
* **Significance:**

  * Shows how many brokers exist in the source/destination cluster that this agent can see.
  * Useful for monitoring cluster membership.

---

### **4. `redpanda_edge_agent_source_topics_total`** & **5. `redpanda_edge_agent_destination_topics_total`**

* **Type:** Gauge
* **Unit:** Count
* **Scope:** Per pod
* **Significance:**

  * Total number of topics in each cluster the agent can detect.
  * Can help detect if topics are missing or misconfigured.

---

### **6. `redpanda_edge_agent_source_partitions_total`** & **7. `redpanda_edge_agent_destination_partitions_total`**

* **Type:** Gauge
* **Unit:** Count (partitions)
* **Scope:** Per pod
* **Significance:**

  * Total partitions across all topics in each cluster.
  * Important for load and replication monitoring.

---

### **8. `redpanda_edge_agent_uncommitted_offsets_total`**

* **Type:** Gauge
* **Unit:** Count (number of partitions with uncommitted offsets)
* **Scope:** Per pod
* **Significance:**

  * Tracks how many partitions are “behind” in terms of consumed messages.
  * Helps detect lag buildup for consumers.

---

### **9. `redpanda_edge_agent_partitions_per_topic`**

* **Type:** Gauge
* **Unit:** Count (partitions per topic)
* **Labels:** `pod`, `cluster`, `topic`
* **Scope:** Per pod, per topic
* **Significance:**

  * Shows **partition distribution per topic**.
  * Useful for troubleshooting if some topics are under-provisioned.

---

### **10. `redpanda_edge_agent_source_ping_ms`** & **11. `redpanda_edge_agent_destination_ping_ms`**

* **Type:** Gauge
* **Unit:** Milliseconds
* **Scope:** Per pod
* **Significance:**

  * Measures latency to source/destination cluster.
  * High values indicate networking or cluster responsiveness issues.

---

### **12. `redpanda_edge_agent_metrics_last_scrape_unix`**

* **Type:** Gauge
* **Unit:** Unix timestamp
* **Scope:** Per pod
* **Significance:**

  * Indicates when the last metrics scrape was performed.
  * Useful to ensure the agent is still actively pushing metrics.

---

### **13. `redpanda_edge_agent_total_lag`**

* **Type:** Gauge
* **Unit:** Count (number of messages)
* **Scope:** Per pod
* **Significance:**

  * Total consumer lag across all partitions.
  * Useful to detect delayed replication.

---

### **14. `redpanda_edge_agent_max_lag`**

* **Type:** Gauge
* **Unit:** Count (number of messages)
* **Scope:** Per pod
* **Significance:**

  * Largest lag in any partition.
  * Highlights worst-case delay scenario.

---

### **15. `redpanda_edge_agent_committed_offset`**

* **Type:** Gauge
* **Unit:** Count (Kafka offset)
* **Labels:** `pod`, `cluster`, `topic`, `partition`
* **Scope:** Per pod, per partition
* **Significance:**

  * Shows the latest committed offset for each partition.
  * Helps track exactly how far a consumer has progressed.

---

### **16. `redpanda_edge_agent_consumer_bytes_total`** & **17. `redpanda_edge_agent_producer_bytes_total`**

* **Type:** Counter
* **Unit:** Bytes
* **Scope:** Per pod
* **Significance:**

  * `consumer_bytes_total`: Total bytes read by agent from source cluster
  * `producer_bytes_total`: Total bytes sent to destination cluster
  * Useful for monitoring network and throughput.

---

### **18. `redpanda_edge_agent_records_fetched_total`** & **19. `redpanda_edge_agent_records_sent_total`**

* **Type:** Counter
* **Unit:** Count (messages)
* **Scope:** Per pod
* **Significance:**

  * Tracks number of records fetched and sent.
  * Helps monitor agent activity and throughput.

---

### **20. `redpanda_edge_agent_fetch_errors_total`** & **21. `redpanda_edge_agent_produce_errors_total`** & **22. `redpanda_edge_agent_commit_errors_total`**

* **Type:** Counter
* **Unit:** Count (errors)
* **Scope:** Per pod
* **Significance:**

  * `fetch_errors_total`: Number of fetch errors from source
  * `produce_errors_total`: Number of errors producing to destination
  * `commit_errors_total`: Errors committing offsets
  * Critical for alerting on replication or connectivity issues.

---

### **23. `redpanda_edge_agent_backoff_duration_seconds`**

* **Type:** Histogram
* **Unit:** Seconds
* **Scope:** Per pod
* **Significance:**

  * Measures how long the agent is backing off due to repeated errors.
  * Helps tune backoff and identify clusters with frequent failures.

---

### **24. `redpanda_edge_agent_partition_lag`**

* **Type:** GaugeVec
* **Unit:** Messages (Count)
* **Scope:** Per pod, per cluster, per topic, per partition
* **Significance:**

  * Tells how far behind the consumer is for each partition
  * Low or zero lag: Your consumer is keeping up — good performance
  * Increasing lag: The consumer (edge-agent) is falling behind the producer; possibly due to network issues, broker throttling, or insufficient consumer throughput. 
  * Consistently high lag: Indicates potential data delivery delays or bottlenecks in your replication/streaming pipeline

---

### **25. `redpanda_edge_agent_metrics_last_scrape_age_seconds`**

* **Type:** GaugeVec
* **Unit:** Seconds
* **Scope:** Per pod
* **Significance:**

  * Represents how long ago (in seconds) the last successful metrics collection or Prometheus scrape occurred for that pod.
  * A low value (close to 0) means scrapes are happening frequently and on time.
  * A high value means the agent hasn’t been scraped or hasn’t successfully collected metrics recently — often a sign of network, liveness, or exporter issues.
  * Useful for alerting if no new scrapes occur for a defined period (e.g. alert if > 60s).

---
