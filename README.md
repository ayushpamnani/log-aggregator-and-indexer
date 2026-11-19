# Log Aggregator & Indexer

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Stdlib%20only-No%20deps-success?style=for-the-badge" alt="No deps">
  <img src="https://img.shields.io/badge/Tests-15%20passing-brightgreen?style=for-the-badge" alt="Tests">
  <img src="https://img.shields.io/badge/Deploy-Render-blue?style=for-the-badge" alt="Render">
</p>

A **production-quality, single-node log aggregation and full-text indexing system** built entirely with Python's standard library — no third-party packages required.

Designed as an end-semester system design project demonstrating: write-ahead logging, LSM-tree style segment storage, inverted index search, backpressure, compaction, retention, and a modern observability dashboard.

---

## 🌐 Live Demo

> **Deployed on Render:** *(deploy to get your URL)*  
> Send logs: `curl -X POST https://your-app.onrender.com/ingest -H "Content-Type: application/json" -d '{"message":"hello world","level":"INFO","service":"demo"}'`

---

## 📋 Problem Statement

In a microservice system, each service writes logs on its own machine. Debugging an incident means SSH-ing into many hosts and grepping files manually. We need **one centralized system** where logs from all services are:

- **Collected** (pushed over HTTP from any host)
- **Stored durably** (survives process crashes via WAL)
- **Indexed** (full-text inverted index, like a mini Elasticsearch)
- **Searchable** (keyword, phrase, prefix, exclusion, field filters, boolean)
- **Visualized** (real-time dashboard with charts)

---

## ✨ Features

| Feature | Detail |
|---------|--------|
| **HTTP ingestion API** | JSON objects, arrays, plain text — any format |
| **Write-ahead log (WAL)** | Durability: acknowledged logs survive crashes |
| **Inverted index** | TF-IDF ranked full-text search |
| **Field filters** | `level:error`, `service:payment-service`, `host:node-1` |
| **Phrase search** | `"connection refused"` |
| **Prefix search** | `time*` matches timeout, timestamp, … |
| **Exclusion** | `-healthcheck` removes noisy logs |
| **Boolean OR** | `a OR b` |
| **Segment storage** | LSM-tree style: gzip-compressed, atomic writes |
| **Compaction** | Merges small segments, bounds segment count |
| **Retention** | Auto-deletes segments older than N hours |
| **Backpressure** | HTTP 429 when queue is full; agents retry |
| **API-key auth** | Protects `/ingest` and `/flush` |
| **Modern dashboard** | Dark mode, Chart.js visualizations, real-time updates |
| **Log agent** | Synthetic traffic generator or file tailer |
| **15 automated tests** | Engine, WAL recovery, search, server auth |

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                     LOG PRODUCERS                             │
│   [App Agent]   [File Tailer]   [curl / HTTP client]          │
│         │               │               │                     │
│         └───────────────┴───────────────┘                     │
│                   POST /ingest (HTTP)                         │
└───────────────────────────┬───────────────────────────────────┘
                            │
┌───────────────────────────▼───────────────────────────────────┐
│                    INGEST API (server.py)                     │
│  parse → validate → assign ID → WAL append → enqueue → 202   │
└──────────────┬──────────────────────────┬─────────────────────┘
               │ WAL                      │ bounded queue
  ┌────────────▼──────┐       ┌───────────▼────────────────────┐
  │   wal.log         │       │        INDEXER THREAD          │
  │ (append-only JSON)│       │  drain batch → memtable.add()  │
  └───────────────────┘       │  → flush if threshold reached  │
                              └───────────┬────────────────────┘
                                          │ flush
                              ┌───────────▼────────────────────┐
                              │   SEGMENTS (data/segments/)    │
                              │  seg-000001.json.gz (immutable)│
                              │  seg-000002.json.gz            │
                              │  ...                           │
                              │  + compaction + retention      │
                              └───────────┬────────────────────┘
                                          │
┌─────────────────────────────────────────▼──────────────────────┐
│                     QUERY ENGINE                               │
│  GET /search  parse_query → prune segments → search → rank     │
│  GET /timeseries → bucket by minute/hour/day                   │
│  GET /stats  → counters, segment info, queue depth             │
└─────────────────────────────────────────┬──────────────────────┘
                                          │
┌─────────────────────────────────────────▼──────────────────────┐
│                   REACT-LIKE DASHBOARD (index.html)            │
│  Stats cards · Line charts · Doughnut · Bar · Log explorer     │
│  Search · Filters · Pagination · Auto-refresh                  │
└────────────────────────────────────────────────────────────────┘
```

**Data structures:**
- **WAL** — append-only file of JSON lines; replayed on startup
- **Memtable** — mutable `Segment` object (inverted index + field indexes) in memory
- **Segment** — immutable, gzip'd `{docs, postings, by_field}` written atomically
- **Inverted index** — `term → {doc_id: term_frequency}` mapping
- **Field index** — `{service|level|host} → value → set[doc_id]`

---

## 🛠️ Technologies

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.10+ (stdlib only) |
| HTTP server | `ThreadingHTTPServer` (stdlib) |
| Storage | Gzip-compressed JSON segments + WAL |
| Search | Custom TF-IDF inverted index |
| Dashboard | Vanilla HTML/JS + Chart.js (CDN) |
| Testing | `unittest` (stdlib) |
| CI | GitHub Actions |
| Deployment | Render.com (Docker or native Python) |

---

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/ayushpamnani/log-aggregator-and-indexer.git
cd log-aggregator-and-indexer

# No pip install needed — zero dependencies!
# Python 3.10+ required.
python --version  # Verify >= 3.10
```

---

## 🚀 Running Locally

### Start the server

```bash
# Default (original) command:
python -m logagg --host 127.0.0.1 --port 8081 --data-dir data

# With auto-seed (pre-populates 500 demo logs on first run):
python -m logagg --host 127.0.0.1 --port 8081 --data-dir data --seed

# All options:
python -m logagg --help
```

Open **http://127.0.0.1:8081/** for the dashboard.

### Generate synthetic logs (in a second terminal)

```bash
# Continuous synthetic traffic at 100 logs/second:
python -m logagg.agent --generate --url http://127.0.0.1:8081

# Faster traffic:
python -m logagg.agent --generate --rate 500 --url http://127.0.0.1:8081

# Stop after 1000 logs:
python -m logagg.agent --generate --count 1000 --url http://127.0.0.1:8081

# Tail a real log file:
python -m logagg.agent --tail /var/log/syslog --service my-service --url http://127.0.0.1:8081
```

### Ingest logs manually

```bash
# Single JSON log
curl -X POST http://127.0.0.1:8081/ingest \
  -H "Content-Type: application/json" \
  -d '{"message":"Payment timeout after 3000ms","level":"ERROR","service":"payment-service","host":"node-1"}'

# Batch
curl -X POST http://127.0.0.1:8081/ingest \
  -H "Content-Type: application/json" \
  -d '[{"message":"user logged in","level":"INFO","service":"auth-service"},{"message":"slow query","level":"WARN","service":"db"}]'

# Plain text (auto-parsed if it matches the structured format)
curl -X POST http://127.0.0.1:8081/ingest \
  -H "Content-Type: text/plain" \
  -d "2026-01-01T12:00:00Z ERROR [payment-service] Timeout calling gateway"
```

---

## 🔍 Query Syntax

| Syntax | Example | Description |
|--------|---------|-------------|
| Keywords | `timeout gateway` | AND: both words must appear |
| Field filter | `level:error` | Exact match on level/service/host |
| Phrase | `"connection refused"` | Exact phrase in message |
| Prefix | `pay*` | Matches payment, payload, paying… |
| Exclusion | `-healthcheck` | Exclude documents with this word |
| Boolean OR | `auth OR payment` | Union of two groups |
| Combined | `level:error service:payment-service -retry` | Mix any operators |

---

## 📡 API Reference

### `POST /ingest`
Accepts: JSON object, JSON array, `{"logs":[...]}`, or plain text.  
Returns: `202 {"accepted": N, "rejected": M, "errors": [...]}`

### `GET /search`
| Param | Default | Description |
|-------|---------|-------------|
| `q` | `""` | Query string |
| `from` | — | Start time (epoch seconds or ISO-8601) |
| `to` | — | End time |
| `limit` | 50 | Max results (1–500) |
| `offset` | 0 | Pagination offset |
| `sort` | `time` | `time` or `relevance` |

Returns: `{total, took_ms, segments_searched, segments_skipped, facets:{level,service}, results:[...]}`

### `GET /timeseries`
| Param | Default | Description |
|-------|---------|-------------|
| `from` | last hour | Start time |
| `to` | now | End time |
| `bucket` | `minute` | `minute`, `hour`, or `day` |

Returns: `{bucket_seconds, from, to, buckets:[{ts, total, DEBUG, INFO, WARN, ERROR, FATAL}]}`

### `GET /stats`
Returns engine metrics: document counts, segment list, queue depth, WAL size, counters.

### `GET /health`
Returns: `{"status":"ok","uptime_s":42.1}`

### `POST /flush` *(requires X-API-Key header)*
Forces memtable flush to disk.

---

## 🧪 Testing

```bash
# Run all 15 tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_logagg.py -v
python -m pytest tests/test_server.py -v

# Run with coverage
pip install pytest-cov
python -m pytest tests/ --cov=logagg --cov-report=term-missing
```

Expected output: **15 tests passing** across:
- Parsing (tokenizer, dict/text formats, timestamps)
- Query parser (AND, OR, phrase, prefix, exclusion, field)
- Engine search (terms, filters, pagination, compaction)
- WAL recovery (crash simulation + restart)
- Retention (old segment deletion)
- Backpressure (429 behavior)
- Server auth (API key enforcement)

---

## 🌍 Deployment on Render

### Method 1: Render Blueprint (render.yaml — recommended)

1. Push this repository to GitHub
2. Go to [render.com](https://render.com) → **New** → **Blueprint**
3. Connect your GitHub repo
4. Render reads `render.yaml` and deploys automatically

### Method 2: Manual Setup

1. **New Web Service** → Connect GitHub repo
2. Settings:
   - **Runtime**: Python 3
   - **Build command**: `pip install -r requirements.txt`
   - **Start command**: `python -m logagg --host 0.0.0.0 --port $PORT --data-dir /tmp/logagg-data --seed`
3. **Environment variables** (in Render dashboard):
   - `LOGAGG_API_KEY` = any secret string (protects /ingest and /flush)

### Method 3: Docker

```bash
docker build -t logagg .
docker run -p 8080:8080 -v $(pwd)/data:/app/data logagg
```

### ⚠️ Important: Ephemeral Storage on Render Free Tier

Render's **free tier uses ephemeral storage** — `/tmp` is wiped on every restart.

**What this means:**
- Indexed logs are lost on restart
- The `--seed` flag auto-repopulates 500 demo logs on startup
- For a college demo, this is fine — the dashboard will always show data

**For persistent storage:**
- Upgrade to Render Starter plan ($7/mo) + add a **Render Disk**
- Mount it at `/data` and change `--data-dir /data`
- The WAL and segments survive restarts

---

## 🔐 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | Auto (Render) | Render injects this automatically |
| `LOGAGG_API_KEY` | Optional | Protects /ingest and /flush endpoints |
| `HOST` | Optional | Bind address (default: 127.0.0.1 locally, set to 0.0.0.0 on Render) |
| `DATA_DIR` | Optional | Data directory path |

> ⚠️ **Never commit `LOGAGG_API_KEY` to Git.** Set it only in Render's dashboard environment variables.

---

## 📁 Project Structure

```
log-aggregator-and-indexer/
├── logagg/
│   ├── __init__.py
│   ├── __main__.py         # Entry point: python -m logagg
│   ├── server.py           # HTTP API + routing
│   ├── engine.py           # Core: ingestion, indexing, search, timeseries
│   ├── index.py            # Segment = inverted index + field indexes
│   ├── models.py           # LogEntry dataclass + parsing
│   ├── query.py            # Query language parser
│   ├── tokenizer.py        # Text tokenizer (shared by indexer + parser)
│   ├── wal.py              # Write-ahead log
│   ├── agent.py            # Log shipper / traffic generator
│   └── web/
│       └── index.html      # Dashboard (Chart.js + vanilla JS)
├── tests/
│   ├── test_logagg.py      # 13 engine/model/query tests
│   └── test_server.py      # 2 server auth tests
├── docs/
│   └── DESIGN.md           # Detailed system design document
├── scripts/
│   └── benchmark.py        # Performance benchmark
├── Dockerfile
├── render.yaml             # Render.com deployment config
├── requirements.txt        # Empty (stdlib only)
├── .github/workflows/ci.yml
├── .gitignore
└── README.md
```

---

## 📊 Performance

| Metric | Value |
|--------|-------|
| Write throughput | ~20,000 logs/sec (single node) |
| Search latency | <10ms for most queries |
| Memory usage | ~200 bytes per log entry |
| Storage (compressed) | ~50-80% smaller than raw JSON |

---

## 🔮 Future Work / Scaling

1. **Kafka** between agents and indexer (replace WAL + bounded queue)
2. **Shard** by time or service hash across multiple indexer nodes
3. **Stateless query coordinators** fan out across shards
4. **Binary segment format** (faster than JSON + gzip)
5. **Prometheus metrics** endpoint
6. **Alert rules** (threshold on error rate)
7. **Multi-tenant** auth per service

See [docs/DESIGN.md](docs/DESIGN.md) for the full system design document.

---

## 📄 License

MIT — see [LICENSE](LICENSE)
