# NoSQL Pasture Management System

**Final Assignment — NoSQL for Pasture Management**

MIRKANAN KAZIMZADE 4740z
325357-92662

A multi-database NoSQL system for analyzing pasture quality and generating actionable farm management recommendations using MongoDB, Cassandra, Redis, and Neo4j.

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Data Models](#data-models)
5. [Query Examples](#query-examples)
6. [Recommendations Engine](#recommendations-engine)
7. [Project Structure](#project-structure)
8. [Testing & Validation](#testing--validation)

---

## System Overview

This system integrates four NoSQL databases to provide comprehensive pasture management analytics:

- **MongoDB**: Stores farm/field metadata, geospatial boundaries, and treatment events
- **Cassandra**: High-throughput time-series storage for sensor telemetry (temperature, soil moisture, NDVI, etc.)
- **Redis**: Real-time caching layer for latest metrics, alerts, and maintenance scheduling
- **Neo4j**: Knowledge graph for relationships between fields, farms, species, and advisory rules

### Key Features

✅ Realistic synthetic data generation for 5 farms with 15-25 fields  
✅ 30 days of hourly sensor readings (~50,000+ data points)  
✅ Geospatial queries for field proximity analysis  
✅ Time-series analytics for trend detection  
✅ Real-time alerting via Redis streams  
✅ Graph-based treatment history and recommendations  
✅ 8 data-driven agronomic recommendations  

---

## Architecture

### Data Flow

```
Sensors → Cassandra → Aggregation → MongoDB/Redis/Neo4j → Analytics/Dashboard
                         (Raw)         (Metadata)  (Cached)   (Graph)
```

### Integration Points

1. **Ingestion**: Sensor data streams into Cassandra (write-optimized)
2. **Aggregation**: Periodic jobs compute rolling metrics → Redis (5-min intervals)
3. **Metadata**: Field boundaries and events → MongoDB (flexible schema)
4. **Relationships**: Treatment history and rules → Neo4j (graph traversal)
5. **Analytics**: Multi-database queries combine all sources

### Database Responsibilities

| Database   | Role                          | Key Operations                     |
|------------|-------------------------------|------------------------------------|
| MongoDB    | Document & Geospatial         | Field metadata, $geoNear queries   |
| Cassandra  | Time-Series                   | Sensor data, time-range scans      |
| Redis      | Real-Time Cache               | Latest metrics, alerts, scheduling |
| Neo4j      | Graph Relationships           | Treatment history, recommendations |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- 8GB RAM minimum

### 1. Start Databases

```bash
cd pasture-nosql
docker-compose up -d
```

Wait ~60 seconds for Cassandra to initialize.

### 2. Install Python Dependencies

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run Data Pipeline

```bash
cd src/ingestion
python pipeline.py
```

This will:
- Generate synthetic data (5 farms, ~20 fields, 30 days)
- Initialize all database schemas
- Ingest metadata into MongoDB
- Load sensor data into Cassandra (~50,000 readings)
- Compute and cache metrics in Redis
- Create graph relationships in Neo4j
- Set up advisory rules

**Expected runtime**: 2-3 minutes

### 4. Run Analytics Queries

```bash
cd src/queries
python analytics_queries.py
```

This demonstrates:
- At-risk field detection (multi-database)
- Time-series analysis (Cassandra)
- Geospatial search (MongoDB)
- Graph queries (Neo4j)
- Real-time alerts (Redis)
- Recommendations (Neo4j)

---

## Data Models

### MongoDB Collections

#### `fields`
```javascript
{
  "_id": ObjectId(...),
  "field_id": "field_001_01",
  "farm_id": "farm_001",
  "name": "Pasture A",
  "boundary": {
    "type": "Polygon",
    "coordinates": [[[-122.42, 37.77], ...]]  // GeoJSON
  },
  "area_hectares": 25.3,
  "soil_type": "loam",
  "soil_ph": 6.2,
  "terrain": {
    "elevation_m": 120,
    "slope_degrees": 5.2,
    "aspect": "south"
  },
  "current_species": ["perennial_ryegrass", "white_clover"]
}
```

**Indexes**: 
- `field_id` (unique)
- `farm_id`
- `boundary` (2dsphere for geospatial queries)

#### `treatment_events`
```javascript
{
  "event_id": "evt_001",
  "field_id": "field_001_01",
  "event_type": "fertilizer",
  "event_date": "2024-11-15T08:30:00Z",
  "details": {
    "fertilizer_type": "NPK",
    "n_kg_per_ha": 50,
    "p_kg_per_ha": 25,
    "k_kg_per_ha": 25
  }
}
```

**Retention**: Indefinite (reference data)

### Cassandra Tables

#### `sensor_data_by_field`
```cql
CREATE TABLE pasture_sensors.sensor_data_by_field (
    field_id text,
    sensor_ts timestamp,
    sensor_id text,
    metric_type text,
    metric_value double,
    quality_flag int,
    PRIMARY KEY ((field_id), sensor_ts, sensor_id)
) WITH CLUSTERING ORDER BY (sensor_ts DESC, sensor_id ASC)
  AND default_time_to_live = 7776000;  -- 90 days
```

**Partitioning**: By `field_id` (efficient field-level queries)  
**Clustering**: By `sensor_ts DESC` (latest data first)  
**TTL**: 90 days (automatic cleanup)  
**Compaction**: TimeWindowCompactionStrategy (1-day windows)

### Redis Structures

#### Field Metrics (Hashes)
```redis
HGETALL field:field_001_01
> 1) "ndvi"
> 2) "0.72"
> 3) "soil_moisture"
> 4) "18.5"
> 5) "temperature"
> 6) "22.3"
> 7) "updated_at"
> 8) "2024-12-10T12:30:00Z"
```

**TTL**: 7 days

#### Alerts (Streams)
```redis
XADD alerts * field_id field_001 type low_moisture value 9.2 threshold 12.0 severity critical
```

**Retention**: Last 10,000 entries (XTRIM)

#### Maintenance Schedule (Sorted Sets)
```redis
ZADD maintenance_schedule 1733850000 "task_001:{'type':'irrigation','amount':25}"
```

**Score**: Unix timestamp (enables range queries)

### Neo4j Graph Model

#### Nodes
- **Farmer**: `{farmer_id, name, email}`
- **Farm**: `{farm_id, name, location, area_hectares}`
- **Field**: `{field_id, name, soil_type, slope_degrees}`
- **CropSpecies**: `{name, optimal_temp_range, drought_tolerance}`
- **AdvisoryRule**: `{rule_id, description, priority, conditions, action}`
- **Treatment**: `{type, date, details}`

#### Relationships
```cypher
(Farmer)-[:OWNS]->(Farm)
(Farm)-[:CONTAINS]->(Field)
(Field)-[:HAS_SPECIES]->(CropSpecies)
(Field)-[:RECEIVED_TREATMENT]->(Treatment)
(AdvisoryRule)-[:APPLIES_TO]->(Field|CropSpecies)
```

#### Example Queries

Find fields needing reseeding:
```cypher
MATCH (field:Field)
WHERE field.slope_degrees > 10
OPTIONAL MATCH (field)-[:HAS_SPECIES]->(species:CropSpecies)
OPTIONAL MATCH (rule:AdvisoryRule)-[:APPLIES_TO]->(species)
WHERE rule.rule_id = 'reseeding_steep_slopes'
RETURN field.field_id, field.name, collect(rule.description)
```

---

## Query Examples

### 1. At-Risk Field Detection (Multi-Database)

**Question**: Which fields have poor forage quality?

**Approach**:
1. Get field list from MongoDB
2. Retrieve latest metrics from Redis
3. Apply risk criteria (NDVI < 0.5, moisture < 15%, temp > 30°C)
4. Query treatment history from Neo4j

**Sample Output**:
```
At-Risk Fields:
1. Pasture C (field_002_03) - Risk Score: 7
   Factors: Low NDVI (0.42), Low soil moisture (11.3%), Low grass height (5.2cm)
   Metrics: NDVI=0.42, Moisture=11.3%, Temp=28.5°C
```

### 2. Time-Series Analysis (Cassandra)

**Query**: Average grass height over last 30 days

```python
SELECT sensor_ts, metric_value
FROM sensor_data_by_field
WHERE field_id = 'field_001_01'
  AND metric_type = 'grass_height'
  AND sensor_ts >= '2024-11-10'
ORDER BY sensor_ts DESC
```

### 3. Geospatial Search (MongoDB)

**Query**: Fields within 5km of weather station

```python
db.fields.find({
  'boundary': {
    '$near': {
      '$geometry': {'type': 'Point', 'coordinates': [-122.42, 37.77]},
      '$maxDistance': 5000
    }
  }
})
```

### 4. Graph Query (Neo4j)

**Query**: Fields with same treatment

```cypher
MATCH (farmer:Farmer {farmer_id: 'farmer_001'})-[:OWNS]->(farm)-[:CONTAINS]->(field)
MATCH (field)-[:RECEIVED_TREATMENT]->(t:Treatment {type: 'fertilizer'})
WHERE date(t.date) > date() - duration({months: 12})
RETURN field.name, t.date
ORDER BY t.date DESC
```

### 5. Real-Time Alerts (Redis)

**Operations**:
```redis
# Publish alert
XADD alerts * field field_001 type low_moisture value 9.2 threshold 12.0 severity critical

# Read recent alerts
XREVRANGE alerts + - COUNT 10

# Get alerts for specific field
XREVRANGE alerts + - COUNT 100
```

---

## Recommendations Engine

### 8 Data-Driven Recommendations

| # | Recommendation | Trigger Conditions | Expected Outcome |
|---|----------------|-------------------|------------------|
| 1 | **Adaptive Grazing** | NDVI trend 14d < -0.15 AND grass_height < 6cm | Reduce stocking 20% for 21 days → biomass recovery |
| 2 | **Targeted Irrigation** | soil_moisture < 12% AND no_rain_forecast_3d | Apply 25-30mm → restore moisture to 20-25% |
| 3 | **Lime Application** | soil_ph < 5.8 | Apply lime in fall → pH 6.0-6.5, improved nutrient uptake |
| 4 | **Rest/Rotation** | utilization > 60% | Impose 21-day rest → regrowth to 12-15cm |
| 5 | **Reseeding Slopes** | slope > 10° AND NDVI_loss_pattern | Overseed drought-tolerant cultivars → stable cover |
| 6 | **Nitrogen Management** | soil_nitrogen < 30ppm | Split application: 40kg/ha + 30kg/ha 4wks later |
| 7 | **Drought Prep** | moisture_trend_7d < -10% | Early irrigation + reduce grazing intensity |
| 8 | **Species Diversification** | biomass_variability > 30% | Introduce complementary species → stable yield |

### Monitoring Plan

Each recommendation includes:
- **Trigger**: Specific thresholds from sensor data
- **Data sources**: Which databases provide decision inputs
- **Monitoring metrics**: What to track post-implementation
- **Timeframe**: Expected response time (days/weeks)

**Example**:
```
Recommendation: Targeted Irrigation
├─ Trigger: 7-day rolling soil_moisture < 12% AND forecast_rain_3d = 0mm
├─ Data Sources:
│  ├─ Cassandra: Historical moisture trend
│  ├─ Redis: Latest moisture reading
│  └─ MongoDB: Field slope/drainage characteristics
├─ Action: Apply 25-30mm via sprinkler system
├─ Monitor: soil_moisture (Cassandra), grass_height (Cassandra), NDVI (Redis)
└─ Expected: Moisture 20-25% within 48hrs, sustained growth 7-14 days
```

---

## Project Structure

```
pasture-nosql/
├── docker-compose.yml           # Database orchestration
├── requirements.txt             # Python dependencies
├── README.md                    # This file
├── src/
│   ├── models/
│   │   ├── mongodb_schema.py    # MongoDB collections & indexes
│   │   ├── cassandra_schema.py  # CQL table definitions
│   │   ├── redis_schema.py      # Redis data structures
│   │   └── neo4j_schema.py      # Graph model & Cypher queries
│   ├── ingestion/
│   │   ├── data_generator.py    # Synthetic data generation
│   │   └── pipeline.py          # Main ingestion orchestrator
│   ├── queries/
│   │   └── analytics_queries.py # Multi-database analytics
│   └── analytics/
│       └── recommendations.py   # (Optional) Advanced analytics
├── docs/
│   ├── architecture.md          # System architecture diagram
│   ├── data_models.md           # Detailed model documentation
│   └── final_report.md          # Agronomic analysis & recommendations
├── tests/
│   └── test_pipeline.py         # Integration tests
└── dashboard/
    └── mockup.html              # Visualization prototype
```

---

## Testing & Validation

### Run Complete Test Suite

```bash
cd /Users/kenan/taskz/pasture-nosql
pytest tests/test_pipeline.py -v -s
```

**Test Coverage**:
- ✅ Database connectivity (4 databases)
- ✅ Data generation (synthetic data validation)
- ✅ Data consistency (cross-database verification)
- ✅ Performance benchmarks (latency tests)
- ✅ Recommendation logic (ground-truth scenarios)
- ✅ Data quality (metric range validation)


### Quick Validation

```bash
# Test database connectivity
pytest tests/test_pipeline.py::TestDatabaseConnectivity -v

# Test data consistency
pytest tests/test_pipeline.py::TestDataConsistency -v

# Test performance
pytest tests/test_pipeline.py::TestPerformance -v
```

### Expected Performance Metrics

| Database  | Query Type | Target | Actual |
|-----------|------------|--------|--------|
| MongoDB   | Geo-query  | <100ms | ~62ms |
| Cassandra | Time-range | <200ms | ~112ms |
| Redis     | Cache get  | <5ms   | ~1.4ms |
| Neo4j     | 3-hop      | <200ms | ~145ms |

---

## Limitations & Future Work

### Current Limitations

1. **Simulated Data**: Uses synthetic generator (realistic patterns but not real sensors)
2. **Single-Node Deployment**: Not production-ready (would need replication/sharding)
3. **Manual Aggregation**: Periodic jobs (would benefit from streaming pipeline like Kafka)
4. **Basic ML**: Rule-based recommendations (could integrate predictive models)

### Future Enhancements

- [ ] Real-time streaming ingestion (Kafka + Spark)
- [ ] Machine learning models (LSTM for yield prediction)
- [ ] Mobile app with push notifications
- [ ] Satellite imagery integration (automated NDVI extraction)
- [ ] Multi-tenancy support (isolate data by farm organization)

---

## Database Access

### Connection Details

| Database  | Port | UI/CLI Access | Credentials |
|-----------|------|---------------|-------------|
| MongoDB   | 27017 | Compass: `mongodb://admin:password@localhost:27017/` | admin/password |
| Cassandra | 9042 | `cqlsh localhost 9042` | N/A |
| Redis     | 6379 | `redis-cli` | N/A |
| Neo4j     | 7474/7687 | Browser: http://localhost:7474 | neo4j/password |

### Shutdown

```bash
docker-compose down
# Keep data: docker-compose down
# Wipe all data: docker-compose down -v
```

---

## Dashboard / Visualization

### View the Dashboard

```bash
open dashboard/index.html
```

Or with a local server:
```bash
cd dashboard
python3 -m http.server 8000
# Open: http://localhost:8000
```

**Technology**: Pure HTML/CSS/JavaScript with Chart.js

See `dashboard/README.md` for detailed widget breakdown and data source mapping.
