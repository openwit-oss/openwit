<p align="center">
    <a href="https://openwit.io" target="_blank"><img src="./public/image/heading-banner.png" alt="OpenWit - Distributed Observability Platform" style="width: 100%;"></a>
    <br />
</p>

# OpenWit - Distributed Observability Platform

OpenWit is a high-performance, distributed observability platform designed to ingest, store, and query telemetry data (traces, logs, metrics) at scale. Built in Rust with Apache Arrow and Parquet, it uses a **batch-based architecture** with PostgreSQL for complete pipeline tracking and observability.

## Architecture Overview

OpenWit supports two deployment modes:

### Distributed Mode (Batch-Based Pipeline)

<p align="center">
    <a href="https://docs.openwit.io/" target="_blank"><img src="./public/image/architecture-flow.png" alt="OpenWit - Data Flow Architecture" style="width: 100%;"></a>
    <br />
    <br />
</p>

### Monolith Mode (Single Process)

All components run in a single process for development, testing, or small-scale deployments.

## Batch-Based Architecture

OpenWit uses a **batch-centric design** where data flows through the pipeline in tracked batches:

### Batch Lifecycle

```
1. CREATED      → Batch formed (configurable size/timeout)
2. WAL_WRITTEN  → Written to WAL by source (Kafka/HTTP/gRPC crate) [SAFE & DURABLE]
3. KAFKA_SENT   → Kafka offsets committed (if Kafka source)
4. INGESTED     → Converted to Apache Arrow format
5. STORED       → Written to Parquet files
6. INDEXED      → Indexed for full-text search
7. COMPLETED    → WAL cleanup done
```

**Note**: WAL is written FIRST at the source level, making data immediately recoverable from any downstream failure.

**Batch Configuration**: All batch parameters (size, timeout, max bytes) are fully configurable via YAML. You can tune batch sizes for your specific needs - smaller batches for low latency, larger batches for high throughput.

### PostgreSQL Batch Tracking

Every batch is tracked in PostgreSQL with complete pipeline visibility:

```sql
CREATE TABLE batch_tracker (
    batch_id UUID PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,

    -- Pipeline stage completion flags
    kafka_pip BOOLEAN DEFAULT false,
    wal_pip BOOLEAN DEFAULT false,
    ingestion_pip BOOLEAN DEFAULT false,
    storage_pip BOOLEAN DEFAULT false,
    index_pip BOOLEAN DEFAULT false,
    janitor_pip BOOLEAN DEFAULT false,

    -- Batch metadata
    batch_size INTEGER NOT NULL,
    message_count INTEGER NOT NULL,
    first_offset BIGINT,
    last_offset BIGINT,
    topics TEXT[],

    -- Timing information
    created_at TIMESTAMPTZ DEFAULT NOW(),
    kafka_completed_at TIMESTAMPTZ,
    wal_completed_at TIMESTAMPTZ,
    ingestion_completed_at TIMESTAMPTZ,
    storage_completed_at TIMESTAMPTZ,
    index_completed_at TIMESTAMPTZ,

    -- Error tracking
    error_stage VARCHAR(50),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
)
```

**Benefits:**
- Complete observability of every batch
- Find stuck/failed batches instantly
- Track throughput per client
- Identify pipeline bottlenecks
- Audit trail for compliance

## Components

### Control Plane
- **Service Discovery**: gRPC-based node discovery and health tracking
- **Health Monitoring**: Tracks status of all nodes in the cluster
- **Batch Monitoring**: Centralized batch status API
  - `/api/batch-monitor/status` - Global batch statistics
  - `/api/batch-monitor/batches/{batch_id}` - Individual batch tracking
  - `/api/batch-monitor/alerts` - Stuck batch alerts
- **Node Management**: Registers and manages node lifecycle

### Ingestion Nodes
OpenWit supports three ingestion methods that create batches:

1. **Kafka Consumer**
   - Consumes messages in configurable batches (size, timeout, max bytes)
   - Creates batch_id and registers in PostgreSQL
   - **Writes to WAL immediately** (in Kafka crate)
   - **Safe Commit Strategy**: Only commits Kafka offsets after WAL write
   - Sends batch metadata to ingestion for processing
   - Zero-copy optimizations
   - **Configurable**: batch size, timeout, max bytes, commit strategy

2. **gRPC/OTLP**
   - Native OTLP traces, metrics, and logs services
   - Batches incoming requests (configurable window)
   - **Writes to WAL immediately** (in gRPC crate)
   - Direct OTLP-to-Arrow conversion (12x faster than JSON)
   - Connection pooling and keepalive
   - **Configurable**: max message size, concurrent streams, timeout

3. **HTTP/REST**
   - REST API for OTLP data ingestion
   - Batches incoming requests (configurable window)
   - **Writes to WAL immediately** (in HTTP crate)
   - **Configurable**: max payload size, concurrent requests, timeout
   - CORS and compression support

**Batch Processing Flow:**
```
1. Receive data from Kafka/HTTP/gRPC
2. Form batch (size/timeout configurable via YAML)
3. Write to WAL immediately (in Kafka/HTTP/gRPC crate)
4. Update batch_tracker.wal_pip = true
5. Register batch in PostgreSQL (batch_tracker)
6. Convert OTLP → Arrow format (direct conversion)
7. Send Arrow batch to storage via gRPC
8. Update batch_tracker.ingestion_pip = true
9. Return confirmation to source
```

### Write-Ahead Log (WAL)
- **Location**: Written by ingestion sources (Kafka/HTTP/gRPC crates)
- **Durability**: CRC32 checksums, fsync() for disk persistence
- **Format**: JSON-based with magic header and metadata
- **Organization**: `./data/wal/{client_id}/batch_{uuid}.wal`
- **Safety**: Kafka commits only after WAL write
- **Recovery**: If any downstream step fails, batch can be replayed from WAL
- **Resumption**: System resumes from WAL on restart/failure

**Critical Safety Rule**: WAL is written FIRST before any processing, allowing recovery from any failure point

### Storage Nodes
- **Columnar Storage**: Apache Parquet with ZSTD compression
- **Multi-Tenant**: Client-based data isolation
- **Time Partitioning**: Organized by year/month/day
- **Path Structure**: `parquet/{client}/{year}/{month}/{day}/{node_id}/{filename}`
- **File Rotation**: 200MB or 60 minutes (configurable)
- **Batch Tracking**: Updates `batch_tracker.storage_pip = true`

**Supported Backends:**
- **Local**: File-based storage (default)
- **Azure Blob Storage**: Full Azure integration
- **AWS S3**: S3-compatible (MinIO, LocalStack supported)
- **Google Cloud Storage**: GCS integration

**Parquet File Lifecycle:**
1. Active file: `{client}_{timestamp}_{ulid}_active.parquet`
2. Rotation triggered → rename to `_stable.parquet`
3. Upload to cloud storage (async, 2-minute delay)
4. Optional: Delete local copy after successful upload

### Indexer Nodes
- **Full-Text Search**: Tantivy-based text indexing (Lucene-like)
- **Batch Processing**: Reads parquet files and creates indexes
- **Segment-Based**: Each batch creates search segments
- **Retry Logic**: Max 3 attempts for failed batches
- **Batch Tracking**: Updates `batch_tracker.index_pip = true`

**Indexing Features:**
- Bloom Filters: Fast existence checks (0.01 false positive rate)
- Zone Maps: Min/max value tracking for query pruning
- Columnar Indexes: Optimized for analytical queries
- Bitmap Indexes: For low-cardinality fields

### Search Nodes
- **SQL Query Engine**: Apache DataFusion 48.0
- **Parquet Integration**: Direct parquet file scanning
- **Push-Down Predicates**: Filter optimization at storage layer
- **Time-Range Queries**: Efficient time-based filtering
- **Multi-Tenant Queries**: Client/tenant isolation
- **Index Integration**: Uses Tantivy indexes for full-text search

### Janitor Node
- **WAL Cleanup**: Removes WAL files after successful storage
- **Parquet Optimization**: Compaction and cleanup
- **Retention Management**: Data lifecycle management
- **Batch Tracking**: Updates `batch_tracker.janitor_pip = true`

## Key Features

### Batch-Based Reliability
- **Complete Tracking**: Every batch tracked in PostgreSQL
- **Pipeline Visibility**: See exactly where each batch is
- **Stuck Batch Detection**: Automatic alerts for batches older than 60 minutes
- **Retry Logic**: Failed batches automatically retried (max 3 attempts)
- **Audit Trail**: Full history of batch processing

### Performance
- **Zero-Copy Processing**: Apache Arrow throughout the pipeline
- **Direct Conversion**: OTLP → Arrow → Parquet (no JSON) - 12x faster
- **Configurable Batching**: Tune batch size/timeout for your latency/throughput needs
- **Parallel Processing**: Multi-threaded ingestion and query execution
- **Efficient Storage**: 10:1 compression ratio with ZSTD

### Scalability
- **Horizontal Scaling**: Each node type scales independently
- **Partitioned Data**: Client and time-based partitioning
- **Cloud Storage**: Unlimited capacity with Azure/S3/GCS
- **Distributed Queries**: Scale query execution across nodes

### Safety & Durability
- **WAL-Based**: No data loss even on crashes
- **Safe Kafka Commits**: Only after WAL write confirmation
- **CRC32 Checksums**: Detect corruption in WAL files
- **Transactional**: PostgreSQL ensures batch state consistency
- **Recovery**: Failed batches can be replayed from WAL

### Multi-Tenancy
- **Client Isolation**: Separate storage paths per client
- **Query Filtering**: Automatic client-based filtering
- **Resource Limits**: Configurable per-tenant limits
- **Batch Tracking**: Per-client throughput statistics

### Observability
- **Batch Monitoring API**: Real-time pipeline visibility
- **PostgreSQL Queries**: Ad-hoc analysis of batch flow
- **Performance Metrics**: Latency tracking per pipeline stage
- **Alerts**: Automatic alerts for stuck/failed batches
- **Throughput Stats**: Per-client message rates

## Data Flow

### Typical Batch Flow (Kafka Example)

1. **Kafka Consumer** receives a batch of messages from topic `v6.qtw.traces.client1.0` (batch size configured in YAML)
2. **Batch Created**:
   - `batch_id = "ingest-1-1234567890"`
   - Register in `batch_tracker` table
   - Status: `kafka_pip = true`
3. **WAL Write** (in Kafka crate):
   - Write batch to WAL: `./data/wal/client1/batch_{uuid}.wal`
   - fsync() to ensure disk persistence
   - Update: `wal_pip = true`
   - **Critical**: Data is now durable and recoverable
4. **Kafka Consumer** commits offsets (safe - data is in WAL)
5. **Ingestion Processing**:
   - Read batch data
   - Convert OTLP → Arrow (direct, no JSON)
   - Update: `ingestion_pip = true`
   - Send Arrow batch to storage
6. **Storage Processing**:
   - Write to parquet: `parquet/client1/2025/01/11/storage-1/file_active.parquet`
   - Update: `storage_pip = true`
7. **Indexer Processing**:
   - Read parquet file
   - Create Tantivy search indexes
   - Update: `index_pip = true`
8. **Janitor Cleanup**:
   - Delete WAL file (data safely in parquet)
   - Update: `janitor_pip = true`
9. **Batch Complete**: All pipeline flags = true

**Recovery Scenario**: If ingestion, storage, or indexer fails, the batch is still in WAL and can be replayed from step 5 onwards.

### HTTP/gRPC Flow

Same flow but batching happens at the HTTP/gRPC handlers:
1. HTTP/gRPC requests buffered (timeout configurable)
2. Batch formed based on configured size/timeout/bytes thresholds
3. **WAL written by HTTP/gRPC crate**
4. Update: `wal_pip = true`
5. Rest of pipeline identical to Kafka flow

**Key Point**: All three ingestion sources (Kafka, HTTP, gRPC) write WAL at the source level, ensuring data durability before any downstream processing. All batch parameters are configurable via YAML.

## Technical Stack

- **Language**: Rust 2021 Edition
- **In-Memory Format**: Apache Arrow 55.2
- **Storage Format**: Apache Parquet 55.2 with ZSTD compression
- **Query Engine**: Apache DataFusion 48.0
- **Indexing**: Tantivy 0.22
- **Batch Tracking**: PostgreSQL (via sqlx)
- **Protocols**: gRPC (Tonic), HTTP (Axum), Arrow Flight
- **Cloud Integration**: OpenDAL (Azure, S3, GCS)
- **Metastore**: Sled (embedded) or PostgreSQL

## Getting Started

### Prerequisites
- **Rust 1.70+** (for building from source)
- **PostgreSQL 12+** (for batch tracking)
- **Docker & Docker Compose** (for containerized deployment)
- **Kubernetes 1.24+** (optional, for production deployment)

### Quick Start - Distributed Mode

```bash
# Clone the repository
git clone https://github.com/middleware-labs/openwit.git
cd openwit

# Setup PostgreSQL database
psql -U postgres -c "CREATE DATABASE openwit;"

# Build the project
RUSTFLAGS="--cfg tokio_unstable" cargo build --release

# Terminal 1: Start control plane
./target/release/openwit-cli control --node-id control-1

# Terminal 2: Start ingestion node with Kafka
./target/release/openwit-cli kafka --node-id kafka-1

# Terminal 3: Start ingestion node with gRPC
./target/release/openwit-cli ingest --node-id ingest-1 --force-grpc

# Terminal 4: Start storage node
./target/release/openwit-cli storage --node-id storage-1

# Terminal 5: Start indexer node
./target/release/openwit-cli indexer --node-id indexer-1

# Terminal 6: Start search node
./target/release/openwit-cli search --node-id search-1

# Optional: Start HTTP ingestion
./target/release/openwit-cli http --node-id http-1 --bind 0.0.0.0:9087
```

### Quick Start - Monolith Mode

```bash
# Setup PostgreSQL
psql -U postgres -c "CREATE DATABASE openwit;"

# Build the project
RUSTFLAGS="--cfg tokio_unstable" cargo build --release

# Run in monolith mode (all services in one process)
./target/release/openwit-cli monolith

# Or with custom configuration
./target/release/openwit-cli --config config/openwit-unified-control.yaml monolith
```

### Docker Compose

```bash
# Start all services (includes PostgreSQL)
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f openwit-ingest

# Stop services
docker-compose down
```

### Monitoring Batches

```bash
# Check batch monitoring API
curl http://localhost:8083/api/batch-monitor/status

# Find stuck batches
curl http://localhost:8083/api/batch-monitor/batches?status=pending&min_age_minutes=60

# Get specific batch status
curl http://localhost:8083/api/batch-monitor/batches/{batch_id}

# PostgreSQL queries
psql -U postgres openwit

# Find incomplete batches
SELECT batch_id, client_id, created_at, message_count,
       kafka_pip, wal_pip, ingestion_pip, storage_pip, index_pip
FROM batch_tracker
WHERE NOT janitor_pip
  AND created_at < NOW() - INTERVAL '60 minutes';

# Client throughput stats
SELECT client_id,
       COUNT(*) as batch_count,
       SUM(message_count) as total_messages
FROM batch_tracker
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY client_id;
```

### Sending Test Data

```bash
# Send OTLP traces via gRPC (default port 4317)
# Use any OTLP-compatible client or exporter

# Send via HTTP (default port 9087)
curl -X POST http://localhost:9087/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "my-service"}
        }]
      },
      "scopeSpans": [{
        "spans": [{
          "traceId": "5B8EFFF798038103D269B633813FC60C",
          "spanId": "EEE19B7EC3C1B174",
          "name": "test-operation",
          "kind": 1,
          "startTimeUnixNano": "1234567890000000000",
          "endTimeUnixNano": "1234567891000000000"
        }]
      }]
    }]
  }'

# Check batch was created
psql -U postgres openwit -c "SELECT * FROM batch_tracker ORDER BY created_at DESC LIMIT 1;"
```

## Configuration

OpenWit uses a unified YAML configuration file:

```yaml
# Deployment mode
deployment:
  mode: "standalone"  # or "monolith", "distributed"

# Control plane
control_plane:
  grpc_endpoint: "http://localhost:7019"
  enabled: true

# Ingestion sources
ingestion:
  sources:
    kafka:
      enabled: true
    grpc:
      enabled: true
    http:
      enabled: true

  kafka:
    brokers: "localhost:9092"
    topics:
      - "v6.qtw.traces.*.*"

    # SAFE COMMIT STRATEGY
    commit_strategy:
      mode: "after_wal"              # Only commit after WAL write
      enable_auto_commit: false      # Manual commits for safety
      wait_for_wal: true
      wal_timeout_ms: 10000
      max_retries: 3

    # Batch configuration - tune for your needs
    batching:
      batch_size: 100                # Messages per batch (configurable)
      batch_timeout_ms: 100          # Max wait time (configurable)
      # For low latency: batch_size: 50-100, batch_timeout_ms: 50-100
      # For high throughput: batch_size: 1000-5000, batch_timeout_ms: 1000-5000

    # High-performance consumer options
    high_performance:
      num_consumers: 2               # Number of consumer threads
      processing_threads: 4          # Parallel processing threads
      num_grpc_clients: 2            # gRPC client pool size
      channel_buffer_size: 10000     # Internal buffer size
      fetch_min_bytes: 262144        # Min bytes to fetch (256KB)
      fetch_max_bytes: 8388608       # Max bytes to fetch (8MB)
      max_partition_fetch_bytes: 2097152  # Max per partition (2MB)

  # Batch tracking configuration
  batch_tracker:
    enabled: true
    postgres_url: "postgresql://postgres:password@localhost/openwit"
    wal_directory: "./data/wal"
    batch_config:
      max_size_bytes: 10485760       # 10MB max (configurable)
      max_messages: 10000            # Max messages per batch (configurable)
      max_age_seconds: 30            # Max batch age before force flush

  grpc:
    port: 4317
    bind: "0.0.0.0"
    max_message_size: 209715200      # 200MB (configurable)
    max_concurrent_requests: 10000   # Concurrent request limit
    request_timeout_ms: 2000         # Request timeout
    worker_threads: 4                # Processing threads
    max_concurrent_streams: 1000     # HTTP/2 streams

    # Batching for gRPC (optional)
    batching:
      enabled: true
      max_batch_size: 1000           # Requests per batch
      batch_timeout_ms: 100          # Max wait time

  http:
    port: 9087
    bind: "0.0.0.0"
    max_payload_size_mb: 100         # Max payload (configurable)
    max_concurrent_requests: 5000    # Concurrent requests
    request_timeout_ms: 30000        # Request timeout

    # Batching for HTTP (optional)
    batching:
      enabled: true
      max_batch_size: 1000           # Requests per batch
      batch_timeout_ms: 100          # Max wait time

# Storage backend
storage:
  backend: "local"  # or "azure", "s3", "gcs"
  data_dir: "./data/storage"

  azure:
    enabled: false
    account_name: ""
    container_name: ""
    access_key: ""

  parquet:
    compression_codec: "zstd"
    compression_level: 3
    row_group_size: 1000000

  # File rotation triggers
  file_rotation:
    file_size_mb: 200                # Rotate at 200MB
    file_duration_minutes: 60        # Or after 1 hour
    enable_active_stable_files: true
    upload_delay_minutes: 2

  # Local file cleanup
  local_retention:
    retention_days: 7
    delete_after_upload: true        # Clean up after cloud upload

# Service ports
service_ports:
  control_plane:
    service: 7019
  storage:
    service: 8081
  ingestion:
    grpc: 4317
    http: 9087
  search:
    service: 8083
```

See `config/openwit-unified-control.yaml` for full configuration options.

## Deployment Options

### Local Development (Monolith)
- All services in single process
- Local PostgreSQL for batch tracking
- Local storage backend
- Ideal for testing and development
- Minimal resource requirements

### Standalone Distributed
- Each node type runs independently
- Shared PostgreSQL for batch tracking
- Local service discovery via control plane
- Good for on-premise deployments
- Flexible scaling per component

### Kubernetes Native
- Helm charts available
- Horizontal pod autoscaling
- PostgreSQL operator for batch tracking
- Cloud storage backends (Azure/S3/GCS)
- Production-ready with monitoring

### Docker Compose
- Multi-container setup
- Includes PostgreSQL service
- Pre-configured networking
- Volume persistence
- Easy local distributed testing

## Use Cases

- **Application Performance Monitoring (APM)**: Distributed tracing with OTLP
- **Log Aggregation**: Centralized logging with full-text search
- **Metrics Collection**: Time-series data storage and querying
- **Observability Platform**: Unified traces, logs, and metrics
- **Real-Time Analytics**: Low-latency data processing with SQL queries
- **Compliance & Audit**: Complete batch tracking for regulatory requirements

## Performance Characteristics

- **Ingestion Throughput**: 100k+ spans/second per ingestion node (higher with larger batches)
- **Batch Latency**: Configurable - sub-1 second with small batches, higher throughput with large batches
- **Query Latency**: Sub-second for time-range queries
- **Compression Ratio**: 10:1 typical with ZSTD
- **Storage Format**: Columnar Parquet for efficient scanning
- **Memory Efficiency**: Zero-copy Arrow processing
- **Conversion Speed**: 12x faster with direct OTLP→Arrow (vs JSON)

### Tuning Batch Sizes

**Low Latency (Sub-second)**:
```yaml
batch_size: 50-100
batch_timeout_ms: 50-100
```

**Balanced**:
```yaml
batch_size: 500-1000
batch_timeout_ms: 500-1000
```

**High Throughput (Maximum)**:
```yaml
batch_size: 5000-10000
batch_timeout_ms: 5000-10000
max_size_bytes: 52428800  # 50MB
```

## Batch Monitoring & Debugging

### Finding Stuck Batches

```sql
-- Batches older than 1 hour that haven't completed
SELECT batch_id, client_id, created_at,
       kafka_pip, wal_pip, ingestion_pip, storage_pip, index_pip
FROM batch_tracker
WHERE NOT janitor_pip
  AND created_at < NOW() - INTERVAL '1 hour'
ORDER BY created_at;
```

### Pipeline Bottleneck Analysis

```sql
-- Count batches at each pipeline stage
SELECT
  COUNT(*) as total_batches,
  SUM(CASE WHEN kafka_pip THEN 1 ELSE 0 END) as kafka_complete,
  SUM(CASE WHEN wal_pip THEN 1 ELSE 0 END) as wal_complete,
  SUM(CASE WHEN ingestion_pip THEN 1 ELSE 0 END) as ingestion_complete,
  SUM(CASE WHEN storage_pip THEN 1 ELSE 0 END) as storage_complete,
  SUM(CASE WHEN index_pip THEN 1 ELSE 0 END) as index_complete,
  SUM(CASE WHEN janitor_pip THEN 1 ELSE 0 END) as janitor_complete
FROM batch_tracker
WHERE created_at > NOW() - INTERVAL '1 hour';
```

### Client Throughput

```sql
-- Messages per second by client (last hour)
SELECT
  client_id,
  COUNT(*) as batches,
  SUM(message_count) as total_messages,
  SUM(message_count) / 3600.0 as messages_per_second
FROM batch_tracker
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY client_id
ORDER BY messages_per_second DESC;
```

## Architecture Notes

### Why Batch-Based Architecture?

The batch-centric design provides:
- **Complete Observability**: Track every batch through the pipeline
- **Debugging**: Find exactly where batches get stuck
- **Reliability**: PostgreSQL ensures state consistency
- **Performance**: Small batches (100ms) for low latency
- **Safety**: Safe Kafka commits only after WAL write
- **Audit Trail**: Full history for compliance

### Why PostgreSQL for Batch Tracking?

- **ACID Guarantees**: Transactional consistency for batch state
- **Rich Queries**: SQL for ad-hoc analysis and debugging
- **Performance**: Millions of batches tracked efficiently
- **Mature**: Battle-tested reliability
- **Ecosystem**: Grafana, monitoring tools integrate easily

### Why Direct OTLP→Arrow?

Traditional observability pipelines:
```
OTLP → JSON → Internal Format → Storage
```

OpenWit's optimized pipeline:
```
OTLP → Arrow → Parquet
```

Benefits:
- **12x faster** conversion than JSON path
- **Zero intermediate allocations**
- **Native columnar format** for Parquet
- **Seamless DataFusion integration** for queries

### Why WAL (Write-Ahead Log)?

- **Written at Source**: WAL is written by Kafka/HTTP/gRPC crates before any processing
- **Durability**: Data persisted to disk before Kafka commits or processing begins
- **Recovery**: If ingestion, storage, or indexer fails, replay from WAL
- **Resumption Point**: System can resume from any failure point in the pipeline
- **Safety**: Zero data loss guarantee - data is durable from the moment it enters OpenWit
- **CRC32 Checksums**: Detect corruption in WAL files
- **Fast**: Sequential writes optimized for throughput

**Recovery Flow**:
```
WAL exists → Ingestion failed → Retry from WAL
WAL exists → Storage failed → Retry from WAL
WAL exists → Indexer failed → Retry from WAL
All stages complete → Janitor deletes WAL
```

### Why Configurable Batches?

OpenWit allows you to tune batch sizes for your specific use case:

**Small Batches (50-100 messages, 50-100ms)** - Low Latency:
- **Sub-1 second end-to-end latency**
- Lower memory pressure
- Fast failure detection
- Better load distribution
- Ideal for: Real-time monitoring, alerting

**Medium Batches (500-1000 messages, 500-1000ms)** - Balanced:
- Good throughput with reasonable latency
- Moderate memory usage
- Ideal for: General-purpose observability

**Large Batches (5000-10000 messages, 5-10 seconds)** - High Throughput:
- Maximum ingestion throughput
- Higher memory efficiency per message
- Bulk data processing
- Ideal for: Log aggregation, batch analytics, historical data

**All batch parameters are configurable in YAML** - tune to your specific latency/throughput requirements.

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Code style and standards
- Testing requirements
- Pull request process
- Issue reporting

## Join Our Community

We welcome everyone to our community! Whether you're contributing code or just saying hello, we'd love to hear from you. Here's how you can connect with us:

- Join the conversation on [Discord](https://discord.gg/B6x6tf4F)
- Follow us on [Twitter](https://twitter.com/middleware_labs)
- Check out our [website](https://middleware.io) and [blog](https://middleware.io/blog) for the latest updates
- Watch our [YouTube channel](https://www.youtube.com/@MiddlewareLabs) for video content

## License

OpenWit is licensed under the MIT License.

## Resources

- [Documentation](https://openwit.vercel.app)
- [GitHub Repository](https://github.com/middleware-labs/openwit)

## Security

OpenWit takes security seriously. Before deploying to production, please review our [Security Policy](SECURITY.md).

### Quick Security Checklist

- [ ] Copy `.env.example` to `.env` and fill in your credentials
- [ ] Never commit `.env` or config files with real credentials to git
- [ ] Use environment variables for all sensitive data
- [ ] Enable TLS/SSL for production deployments
- [ ] Use strong passwords for PostgreSQL
- [ ] Rotate cloud storage credentials regularly
- [ ] Review and configure firewall rules
- [ ] Enable audit logging

### Configuration Security

All sensitive configuration uses environment variables:

```yaml
# Example from config file
azure:
  access_key: "${AZURE_STORAGE_ACCESS_KEY}"  # Uses env variable
postgres_url: "${POSTGRES_URL}"              # Uses env variable
```

**Never hardcode credentials in configuration files!**

For detailed security best practices, see [SECURITY.md](SECURITY.md).

### Reporting Security Issues

If you discover a security vulnerability, please email **security@middleware.io**. Do not create public GitHub issues for security vulnerabilities.
