# OpenWit Docker Guide

## Quick Start

### Build the Image

```bash
docker build -t openwit:latest .
```

### Run Single Container

```bash
docker run -d \
  --name openwit \
  -p 7019:7019 \
  -p 4318:4318 \
  -p 50051:50051 \
  -v openwit_data:/openwit/data \
  openwit:latest control
```

### Run Full Stack with Docker Compose

```bash
docker-compose up -d
```

This starts:
- PostgreSQL (metastore)
- Kafka (message queue)
- Control Plane (port 7019)
- Ingestion Node (gRPC on port 50051)
- HTTP Node (REST API on port 4318)
- Storage Node (port 8081, Arrow Flight on 9401)
- Search Node (port 8083)

## Environment Variables

```bash
export POSTGRES_PASSWORD=your_password
export RUST_LOG=debug
docker-compose up -d
```

## Useful Commands

### View Logs

```bash
docker-compose logs -f control-plane
docker-compose logs -f ingestion
```

### Stop All Services

```bash
docker-compose down
```

### Stop and Remove Data

```bash
docker-compose down -v
```

### Rebuild After Code Changes

```bash
docker-compose build --no-cache
docker-compose up -d
```

## Test Ingestion

### HTTP Endpoint

```bash
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "test-service"}
        }]
      },
      "scopeSpans": [{
        "spans": [{
          "traceId": "5B8EFFF798038103D269B633813FC60C",
          "spanId": "EEE19B7EC3C1B174",
          "name": "test-span",
          "kind": 2,
          "startTimeUnixNano": "1234567890000000000",
          "endTimeUnixNano": "1234567891000000000"
        }]
      }]
    }]
  }'
```

## Available Services

| Service | Port | Description |
|---------|------|-------------|
| Control Plane | 7019 | Service discovery & coordination |
| HTTP Ingestion | 4318 | OTLP HTTP endpoint |
| gRPC Ingestion | 50051 | OTLP gRPC endpoint |
| Storage | 8081 | Storage node API |
| Arrow Flight | 9401 | High-speed data transfer |
| Search | 8083 | Query API |
| PostgreSQL | 5432 | Metastore database |
| Kafka | 9092 | Message queue |

## Production Deployment

### With Cloud Storage (Azure)

```bash
docker run -d \
  --name openwit-control \
  -p 7019:7019 \
  -e AZURE_STORAGE_ACCOUNT=myaccount \
  -e AZURE_STORAGE_ACCESS_KEY=mykey \
  -e AZURE_STORAGE_CONTAINER=openwit \
  -v ./config/prod.yaml:/openwit/config/default.yaml:ro \
  openwit:latest control
```

### With External PostgreSQL

```bash
docker run -d \
  --name openwit-control \
  -p 7019:7019 \
  -e POSTGRES_URL=postgresql://user:pass@host:5432/openwit \
  openwit:latest control
```

## Health Checks

```bash
curl http://localhost:7019/health
curl http://localhost:8083/api/health
```

## Troubleshooting

### View Container Logs

```bash
docker logs openwit-control
docker logs -f openwit-ingest
```

### Access Container Shell

```bash
docker exec -it openwit-control /bin/bash
```

### Check Service Discovery

```bash
docker exec openwit-control openwit --version
```

## Update to Latest Version

```bash
git pull origin main
docker-compose build
docker-compose up -d
```
