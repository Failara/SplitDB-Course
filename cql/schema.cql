CREATE KEYSPACE IF NOT EXISTS bess_lab
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 2,
  'dc2': 1
};

USE bess_lab;

CREATE TABLE IF NOT EXISTS telemetry_simple (
    device_id TEXT,
    timestamp TIMESTAMP,
    soc DOUBLE,
    grid_frequency DOUBLE,
    mode TEXT,
    power_output DOUBLE,
    PRIMARY KEY (device_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS telemetry_minute (
    device_id TEXT,
    bucket_minute TIMESTAMP,
    timestamp TIMESTAMP,
    soc DOUBLE,
    grid_frequency DOUBLE,
    mode TEXT,
    power_output DOUBLE,
    PRIMARY KEY ((device_id, bucket_minute), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS telemetry_daily (
    device_id TEXT,
    bucket_date DATE,
    timestamp TIMESTAMP,
    soc DOUBLE,
    grid_frequency DOUBLE,
    mode TEXT,
    power_output DOUBLE,
    PRIMARY KEY ((device_id, bucket_date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS aggregates_hourly (
    device_id TEXT,
    bucket_hour TIMESTAMP,
    avg_soc DOUBLE,
    max_soc DOUBLE,
    min_soc DOUBLE,
    avg_frequency DOUBLE,
    max_frequency DOUBLE,
    min_frequency DOUBLE,
    PRIMARY KEY (device_id, bucket_hour)
) WITH CLUSTERING ORDER BY (bucket_hour DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS frequency_alerts AS
SELECT device_id, bucket_minute, timestamp, grid_frequency
FROM telemetry_minute
WHERE grid_frequency IS NOT NULL
  AND device_id IS NOT NULL
  AND bucket_minute IS NOT NULL
  AND timestamp IS NOT NULL
PRIMARY KEY ((device_id, bucket_minute), grid_frequency, timestamp);

CREATE INDEX IF NOT EXISTS idx_telemetry_minute_frequency ON telemetry_minute (grid_frequency);

CREATE INDEX IF NOT EXISTS idx_aggregates_hourly_device ON aggregates_hourly (device_id);