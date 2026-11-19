# System Design: Log Aggregator & Indexer

## 1. Problem statement
In a microservice system, each service writes logs on its own machine. Debugging an incident means SSH-ing into many
hosts and grepping files. We need one place where logs from all services are **collected, stored durably, indexed and
searchable** within seconds.

## 2. Requirements

**Functional**
1. Collect logs from many services/hosts (push over HTTP; tail files via an agent).
2. Parse and normalise logs (timestamp, level, service, host, message).
3. Full-text search with filters (service, level, host, time range), boolean logic, phrases, prefix.
4. Pagination, ranking (time or relevance) and facets (counts per level/service).
5. Retention: automatically delete old data.

**Non-functional**
| Property | Target (this implementation) | Notes |
|---|---|---|
| Write throughput | high, ~20k logs/s on one node | batching + async indexing |
| Search latency | tens of ms or less | inverted index, segment pruning |
| Durability | acknowledged logs survive a crash | write-ahead log |
| Availability of ingestion | never lose data under load spikes | bounded queue + back-pressure + client retry |
| Freshness | near-real-time (< 1 s) | memtable is searchable right after indexing |
| Consistency | eventual (index lags the WAL slightly) | acceptable for logs |

**Out of scope:** alerting, dashboards, multi-tenant auth, distributed deployment (discussed in section 10).

## 3. Capacity estimation (example)
* 50 services x 10 instances x 20 logs/s = **10,000 logs/s**
* Avg log ~ 200 B -> 2 MB/s -> **~170 GB/day raw**; with gzip (about 5-8x) about **25-35 GB/day** stored
* 7-day retention -> about 200 GB: fits one beefy node; beyond that, shard.
* Reads are far fewer than writes (about 1-10 queries/s) but must be fast during incidents.

This is why the design is **write-optimised** (append-only WAL, in-memory memtable, immutable segments).

## 4. High-level design
See the diagram in the README. Components:

| Component | Responsibility |
|---|---|
| **Agent / shipper** | Runs next to the app; batches logs, retries with exponential backoff, honours 429. |
| **Ingest API** | Validates and parses, assigns monotonically increasing IDs, appends to WAL, enqueues, returns `202`. |
| **WAL** | Append-only file; the source of truth for not-yet-flushed data; replayed on startup. |
| **Bounded queue** | Decouples the (fast) network path from the (slower) indexing path; provides back-pressure. |
| **Indexer** | Single background thread; drains the queue in batches into the memtable. |
| **Memtable** | Mutable in-memory inverted index. Searchable immediately. |
| **Segments** | Immutable, gzip-compressed on-disk indexes (docs + postings). Written atomically (tmp + rename). |
| **Compactor / retention** | Merges small segments; deletes segments entirely older than the retention window. |
| **Query engine** | Parses queries, prunes segments by time, searches in all live segments, merges + ranks. |

## 5. Data model

```
LogEntry { id:int, ts:epoch, service, level, host, message }

Segment {
  docs:      id -> LogEntry
  postings:  term -> { doc_id -> term_frequency }          # inverted index
  by_field:  {service|level|host} -> value -> {doc_ids}    # exact-match filters
  min_ts, max_ts, max_id                                   # for pruning + WAL truncation
}
```
*Why an inverted index?* A scan is O(total data). An inverted index maps a word directly to the documents containing
it, so a query touches only the relevant posting lists and intersects them.

**Ranking:** `score = sum(tf x idf)` with `idf = ln(1 + N/df)` computed per segment (cheap; a global IDF would need
cross-segment statistics).

## 6. Key flows

**Ingest**
1. Agent sends a batch to `POST /ingest`.
2. Each record is parsed/validated; invalid ones are rejected individually (and reported).
3. Under a lock: check queue capacity (else **429**), assign IDs, **append to WAL**, enqueue.
4. Reply `202 Accepted`. The data is durable from this point.
5. Indexer thread dequeues a batch and adds it to the memtable (now searchable).
6. When the memtable reaches `flush_threshold` docs: write a segment file, then **truncate the WAL** up to the flushed max ID.

**Search**
1. Parse the query into OR-groups of AND-ed clauses.
2. Skip every segment whose `[min_ts, max_ts]` does not overlap the requested time range (**segment pruning**).
3. Per segment: look up posting lists, intersect for AND, subtract for NOT, union groups for OR, verify phrases.
4. Merge hits across segments, sort by time or relevance, paginate, compute facets.

**Crash recovery:** load all segment files -> replay WAL entries whose `id > max flushed id` into the memtable. Torn
last lines in the WAL are skipped. No acknowledged log is lost (with `--fsync`, even a power failure is covered).

## 7. Design decisions and trade-offs

| Decision | Alternative | Why |
|---|---|---|
| WAL before ack | ack after indexing | Fast ack with durability; indexing can lag safely. |
| Async indexing via queue | index inline | Absorbs bursts; request latency is independent of index cost. |
| Bounded queue + 429 | unbounded queue | Prevents OOM; pushes back-pressure to agents, which buffer and retry. |
| Immutable segments (LSM idea) | update-in-place index | Sequential writes, no locking for readers, easy atomic swap, trivial retention (delete a file). |
| Compaction | never merge | Keeps segment count (and per-query overhead) bounded. |
| Retention by whole segment | delete per document | O(1) deletion, no index rewrite. |
| Time-based pruning | search everything | Logs are queried mostly by recent time windows. |
| Eventual consistency | strong | Fine for logs; a few ms of index lag is invisible to humans. |
| At-least-once delivery | exactly-once | Agent retries can duplicate a batch; deduplication would need a producer ID + sequence number (future work). |
| fsync optional | always fsync | Throughput vs. durability knob (`--fsync`). |

**CAP view:** a single node trivially has no partitions. In the distributed version (below) we choose **AP** for
ingestion (accept writes even if some replicas are down) and accept eventually consistent reads.

## 8. Failure handling
| Failure | Behaviour |
|---|---|
| Aggregator crashes | WAL + segments restore state on restart; agents retry unacknowledged batches. |
| Aggregator overloaded | Queue fills -> `429 Retry-After` -> agents back off and keep logs buffered. |
| Crash during segment write | Segments are written to `.tmp` then renamed atomically; WAL is only truncated after the rename. |
| Malformed log | Rejected individually; the rest of the batch is still accepted. |
| Disk full | Writes fail loudly; (production: alert + rate limit + emergency retention). |

## 9. Observability
`GET /stats` exposes document counts, segment sizes, queue depth, WAL size and counters (ingested, indexed, flushes,
compactions, rejected, queries, expired segments). The UI polls it. In production, export these as Prometheus metrics.

## 10. Scaling beyond one node (future work)
1. **Buffer layer:** put Kafka between agents and indexers (replayable log, decouples producers/consumers, replaces our WAL + queue).
2. **Shard** by time (daily indices) and/or by hash of `service` across indexer nodes; **replicate** each shard (2-3 copies).
3. **Stateless query coordinators** fan out a query to relevant shards (prune by time), then merge top-K - the same pattern as our per-segment merge.
4. **Tiered storage:** hot (SSD, last 24h) -> warm -> cold (object storage like S3, queried on demand).
5. **Better index format:** binary postings with delta + varint compression, FST term dictionary, mmap'd segments, positions for fast phrase queries, global IDF / BM25.
6. **Ingest hardening:** auth + per-tenant rate limits, schema/PII scrubbing, deduplication IDs.
7. **Alternative approach (Loki-style):** index only labels (service, level, host) and keep log lines as compressed chunks; far cheaper storage, slower free-text search.

## 11. Comparison with real systems
| | logagg | Elasticsearch / ELK | Grafana Loki | Splunk |
|---|---|---|---|---|
| Index | full inverted index | full inverted index (Lucene segments) | labels only | full + proprietary |
| Storage | local gzip segments | local / replicated shards | object storage chunks | indexers + buckets |
| Ingest buffer | WAL + bounded queue | usually Kafka / Logstash | Promtail / Kafka | forwarders |
| Distribution | single node | sharded + replicated | microservices | clustered |

## 12. Known limitations
* Single node; every segment is loaded fully into memory at startup.
* JSON + gzip segments are simple but far less compact/fast than a binary format.
* Per-segment IDF means scores are only approximately comparable across segments.
* Phrase queries are verified by substring check (no term positions stored).
* Flush and compaction hold the engine lock while writing, which briefly blocks searches.
* No deduplication (at-least-once delivery).
