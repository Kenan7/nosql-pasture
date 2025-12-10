"""
Cassandra schema for high-throughput time-series sensor data.
Optimized for write-heavy workloads and time-range queries.
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


# CQL Schema Definitions
KEYSPACE_SCHEMA = """
CREATE KEYSPACE IF NOT EXISTS pasture_sensors
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
"""

SENSOR_DATA_TABLE = """
CREATE TABLE IF NOT EXISTS pasture_sensors.sensor_data_by_field (
    field_id text,
    sensor_ts timestamp,
    sensor_id text,
    metric_type text,
    metric_value double,
    quality_flag int,
    PRIMARY KEY ((field_id), sensor_ts, sensor_id)
) WITH CLUSTERING ORDER BY (sensor_ts DESC, sensor_id ASC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_size': '1',
      'compaction_window_unit': 'DAYS'
  }
  AND default_time_to_live = 7776000;
"""
# TTL = 90 days (7776000 seconds)

AGGREGATED_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS pasture_sensors.aggregated_metrics_by_field (
    field_id text,
    date text,
    hour int,
    metric_type text,
    avg_value double,
    min_value double,
    max_value double,
    sample_count int,
    PRIMARY KEY ((field_id, date), hour, metric_type)
) WITH CLUSTERING ORDER BY (hour DESC, metric_type ASC)
  AND default_time_to_live = 31536000;
"""
# TTL = 365 days (31536000 seconds)

# Metric types we track
METRIC_TYPES = [
    'temperature',        # Celsius
    'humidity',          # Percentage
    'soil_moisture',     # Percentage
    'precipitation',     # mm
    'wind_speed',        # m/s
    'solar_radiation',   # W/m²
    'grass_height',      # cm
    'ndvi',             # Normalized Difference Vegetation Index
    'soil_ph',          # pH
    'soil_nitrogen',    # ppm
    'soil_phosphorus',  # ppm
    'soil_potassium'    # ppm
]


def init_cassandra_schema(session):
    """Initialize Cassandra keyspace and tables."""
    # Create keyspace
    session.execute(KEYSPACE_SCHEMA)
    print("Keyspace created")
    
    # Create tables
    session.execute(SENSOR_DATA_TABLE)
    print("sensor_data_by_field table created")
    
    session.execute(AGGREGATED_METRICS_TABLE)
    print("aggregated_metrics_by_field table created")
    
    print("Cassandra schema initialized successfully")


# Example insert statements
EXAMPLE_INSERT_SENSOR_DATA = """
INSERT INTO pasture_sensors.sensor_data_by_field 
    (field_id, sensor_ts, sensor_id, metric_type, metric_value, quality_flag)
VALUES 
    ('field_001', '2024-12-10 10:30:00', 'sensor_temp_01', 'temperature', 18.5, 1)
"""

EXAMPLE_INSERT_AGGREGATED = """
INSERT INTO pasture_sensors.aggregated_metrics_by_field
    (field_id, date, hour, metric_type, avg_value, min_value, max_value, sample_count)
VALUES
    ('field_001', '2024-12-10', 10, 'soil_moisture', 22.3, 20.1, 24.5, 12)
"""

# Example query for time range
EXAMPLE_QUERY_TIME_RANGE = """
SELECT sensor_ts, metric_type, metric_value 
FROM pasture_sensors.sensor_data_by_field
WHERE field_id = 'field_001'
  AND sensor_ts >= '2024-12-01 00:00:00'
  AND sensor_ts <= '2024-12-10 23:59:59'
ORDER BY sensor_ts DESC
LIMIT 1000
"""


def get_cassandra_session(contact_points=['localhost'], port=9042):
    """Create and return a Cassandra session."""
    cluster = Cluster(contact_points=contact_points, port=port)
    session = cluster.connect()
    return session, cluster


if __name__ == "__main__":
    # Example usage
    session, cluster = get_cassandra_session()
    init_cassandra_schema(session)
    
    # Example: Insert sample data
    session.execute(EXAMPLE_INSERT_SENSOR_DATA)
    session.execute(EXAMPLE_INSERT_AGGREGATED)
    print("Example data inserted")
    
    # Example: Query data
    rows = session.execute(EXAMPLE_QUERY_TIME_RANGE)
    for row in rows:
        print(f"{row.sensor_ts}: {row.metric_type} = {row.metric_value}")
    
    cluster.shutdown()
