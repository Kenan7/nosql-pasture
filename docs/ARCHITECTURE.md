# System Architecture — Pasture Management NoSQL System

## Overview

Multi-database architecture integrating 4 NoSQL technologies for comprehensive pasture analytics.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  Sensor Networks | Farmer Records | Remote Sensing | Weather    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │  Python │
                    │  Data   │
                    │Generator│
                    └─┬──┬──┬─┘
                      │  │  │
        ┌─────────────┘  │  └────────────┐
        │                │               │
        ▼                ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  CASSANDRA   │  │   MONGODB    │  │    NEO4J     │
│              │  │              │  │              │
│ Time-Series  │  │ Metadata &   │  │  Knowledge   │
│ Sensor Data  │  │ Geospatial   │  │    Graph     │
│              │  │              │  │              │
│ 50K+ points  │  │ Fields/Farms │  │ Relationships│
│ 30 days      │  │ GeoJSON      │  │ Rules        │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                  │
       │         ┌───────▼──────────────────┘
       │         │
       └─────────▼─────────┐
            ┌──────────────▼────┐
            │      REDIS         │
            │                    │
            │  Real-Time Cache   │
            │  Alerts & Metrics  │
            │                    │
            └──────────┬─────────┘
                       │
         ┌─────────────┴─────────────┐
         │                           │
         ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│   ANALYTICS     │         │   DASHBOARD     │
│   QUERIES       │         │   (Mockup)      │
│                 │         │                 │
│ Multi-Database  │         │ Field Risk Map  │
│ Risk Assessment │         │ Time-Series     │
│ Recommendations │         │ Alert Table     │
└─────────────────┘         └─────────────────┘
```

## Data Flow

### 1. Ingestion Layer

**Raw Sensor Data → Cassandra**
- High-throughput writes (5,000+ ops/sec)
- Partitioned by `field_id`
- Clustered by timestamp DESC
- TTL: 90 days

**Metadata → MongoDB**
- Farm/field structure
- GeoJSON boundaries
- Treatment events
- Farmer profiles

**Relationships → Neo4j**
- Farm ownership
- Field-species associations
- Treatment history
- Advisory rules

### 2. Aggregation Layer

**Python Jobs (5-min intervals)**
```
Cassandra raw data
    ↓
Calculate rolling averages (7d, 14d, 30d)
    ↓
Update Redis cache (latest metrics)
    ↓
Trigger alerts if thresholds breached
    ↓
Update Neo4j relationships (if major event)
```

### 3. Query Layer

**Multi-Database Queries**
- **Risk Assessment**: Redis (latest) + MongoDB (metadata) + Neo4j (history)
- **Time-Series**: Cassandra (historical trends)
- **Geospatial**: MongoDB (proximity queries)
- **Recommendations**: Neo4j (graph traversal)

## Database Selection Rationale

### MongoDB — Document & Geospatial

**Why MongoDB?**
- ✅ Flexible schema for farm/field metadata
- ✅ Native GeoJSON support (`2dsphere` index)
- ✅ Fast document updates (treatment events)
- ✅ Rich query language (aggregation pipeline)

**Use Cases**:
- Store field boundaries as polygons
- Query fields within radius of weather station
- Manage treatment event logs
- Store farmer preferences/profiles

### Cassandra — Time-Series

**Why Cassandra?**
- ✅ Optimized for high write throughput
- ✅ Time-based partitioning (efficient range queries)
- ✅ Automatic data compaction (TimeWindowCompactionStrategy)
- ✅ Built-in TTL for data expiration

**Use Cases**:
- Store 50,000+ sensor readings
- Query last 30 days of grass height
- Calculate historical averages
- Handle IoT sensor streams

### Redis — Real-Time Cache

**Why Redis?**
- ✅ In-memory speed (<1ms latency)
- ✅ Rich data structures (hashes, streams, sorted sets)
- ✅ Pub/Sub for real-time alerts
- ✅ TTL for auto-expiring cache

**Use Cases**:
- Cache latest field metrics (HSET)
- Publish alerts (XADD streams)
- Schedule maintenance (ZADD sorted sets)
- Cache expensive query results

### Neo4j — Knowledge Graph

**Why Neo4j?**
- ✅ Native graph storage & traversal
- ✅ Cypher query language (intuitive)
- ✅ Fast relationship queries (3-hop < 100ms)
- ✅ Perfect for recommendation engines

**Use Cases**:
- Model farm-field-farmer relationships
- Find fields with same treatment history
- Match advisory rules to field conditions
- Traverse species-specific recommendations

## Scaling Considerations

### Current Setup (Single-Node)
- **MongoDB**: Single instance
- **Cassandra**: Single node
- **Redis**: Single instance
- **Neo4j**: Single instance

### Production Scaling

**MongoDB**:
- Replica set (3+ nodes)
- Sharding by `farm_id` (geographic distribution)

**Cassandra**:
- Multi-datacenter replication
- 3+ nodes per DC
- RF=3 for durability

**Redis**:
- Redis Cluster (sharding)
- Redis Sentinel (HA)
- Read replicas for queries

**Neo4j**:
- Causal cluster (core + read replicas)
- Fabric for multi-database

## Performance Characteristics

### Write Performance

| Database  | Throughput | Latency | Use Case |
|-----------|------------|---------|----------|
| Cassandra | 5,000+ w/s | 5-10ms  | Sensor ingestion |
| MongoDB   | 1,000 w/s  | 10-20ms | Events |
| Redis     | 100,000 w/s| <1ms    | Cache updates |
| Neo4j     | 100 w/s    | 20-50ms | Relationships |

### Query Performance

| Database  | Query Type | Latency | Notes |
|-----------|------------|---------|-------|
| MongoDB   | Geo-query  | <50ms   | 2dsphere index |
| Cassandra | Time-range | <100ms  | Partition-aware |
| Redis     | Hash get   | <1ms    | In-memory |
| Neo4j     | 3-hop      | <100ms  | Native graph |

## Data Consistency

### Consistency Model

**MongoDB**: Strong consistency (default)
**Cassandra**: Tunable (use CL=QUORUM for prod)
**Redis**: Strong consistency (single-node)
**Neo4j**: ACID transactions

### ETL Strategy

1. **Cassandra → Redis** (5-min batch)
   - Read latest metrics per field
   - Aggregate and update Redis hashes
   - Trigger alerts if needed

2. **Cassandra → MongoDB** (daily)
   - Compute daily/weekly aggregates
   - Store summary stats in MongoDB

3. **MongoDB → Neo4j** (on event)
   - New treatment event → create Treatment node
   - Link to Field via RECEIVED_TREATMENT

## Security

### Authentication

- MongoDB: Username/password (SCRAM-SHA-256)
- Neo4j: Username/password
- Cassandra: Not enabled (use SSL + auth in prod)
- Redis: Not enabled (use ACL in prod)

### Network

- All databases on isolated Docker network
- Expose ports only on localhost
- Use reverse proxy for external access (prod)

## Monitoring & Observability

### Metrics to Track

**Cassandra**:
- Write latency (p95, p99)
- Compaction backlog
- Partition size distribution

**MongoDB**:
- Query latency
- Index usage
- Document growth rate

**Redis**:
- Memory usage
- Cache hit rate
- Eviction rate

**Neo4j**:
- Query execution time
- Node/relationship count
- Store file sizes

### Alerting

- Cassandra write latency > 100ms
- MongoDB geo-query > 200ms
- Redis memory > 80% capacity
- Neo4j query timeout > 30s

## Backup & Recovery

**MongoDB**: 
- mongodump (daily)
- Point-in-time recovery (oplog)

**Cassandra**:
- Snapshot backups (daily)
- Incremental backups (6h)

**Redis**:
- RDB snapshots (1h)
- AOF for durability

**Neo4j**:
- Online backup (daily)
- Transaction logs

---

## Summary

This architecture leverages the strengths of each NoSQL database:

- **Cassandra**: Write-heavy sensor data
- **MongoDB**: Flexible metadata + geospatial
- **Redis**: Ultra-fast caching + alerts
- **Neo4j**: Complex relationship queries

The result is a scalable, performant system that can analyze pasture quality and deliver actionable recommendations to farmers in real-time.
