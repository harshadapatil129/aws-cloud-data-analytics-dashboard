-- ============================================================================
-- Amazon Redshift Schema: Cloud-Based Data Analytics Dashboard
-- Database: analyticsdb
-- Description: Analytical tables, distribution/sort keys, materialized views,
--              and staging tables optimized for high-throughput time-series analytics.
-- ============================================================================

-- Create dedicated analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

-- ----------------------------------------------------------------------------
-- 1. Main Analytics Records Table
-- ----------------------------------------------------------------------------
-- Redshift Optimization:
-- - DISTKEY (source): Evenly distributes records across compute node slices based on data source.
-- - COMPOUND SORTKEY (timestamp, category): Leverages columnar zone maps to prune blocks 
--   for rapid date-range filtering and category aggregation in dashboard queries.
-- - ENCODE specifications: Optimizes columnar compression (az64 for timestamps/numerics, zstd for text).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.analytics_records (
    id                  VARCHAR(64)     NOT NULL    ENCODE zstd,
    timestamp           TIMESTAMP       NOT NULL    DEFAULT SYSDATE ENCODE az64,
    source              VARCHAR(50)     NOT NULL    ENCODE bytedict,
    category            VARCHAR(50)     NOT NULL    ENCODE bytedict,
    value               NUMERIC(14, 2)  NOT NULL    DEFAULT 0.00    ENCODE az64,
    status              VARCHAR(20)     NOT NULL    DEFAULT 'SUCCESS' ENCODE bytedict,
    processing_time_ms  INTEGER         DEFAULT 0   ENCODE az64,
    metadata            VARCHAR(2048)               ENCODE zstd,
    created_at          TIMESTAMP       NOT NULL    DEFAULT SYSDATE ENCODE az64,
    
    PRIMARY KEY (id)
)
DISTSTYLE KEY
DISTKEY (source)
COMPOUND SORTKEY (timestamp, category);

-- ----------------------------------------------------------------------------
-- 2. Dead Letter Queue / Rejected Records Table
-- ----------------------------------------------------------------------------
-- Captures records that failed validation during Lambda processing
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rejected_records (
    rejection_id        VARCHAR(64)     NOT NULL    ENCODE zstd,
    kinesis_sequence_no VARCHAR(128)                ENCODE zstd,
    raw_payload         VARCHAR(4096)               ENCODE zstd,
    error_reason        VARCHAR(512)    NOT NULL    ENCODE zstd,
    source_identifier   VARCHAR(50)                 ENCODE bytedict,
    failed_at           TIMESTAMP       NOT NULL    DEFAULT SYSDATE ENCODE az64,
    
    PRIMARY KEY (rejection_id)
)
DISTSTYLE EVEN
SORTKEY (failed_at);

-- ----------------------------------------------------------------------------
-- 3. Staging Table for Batch Ingestion
-- ----------------------------------------------------------------------------
-- Used for high-volume bulk COPY operations from S3 (via Kinesis Data Firehose)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.stg_kinesis_records (
    id                  VARCHAR(64),
    timestamp           VARCHAR(64),
    source              VARCHAR(50),
    category            VARCHAR(50),
    value               NUMERIC(14, 2),
    status              VARCHAR(20),
    processing_time_ms  INTEGER,
    metadata            VARCHAR(2048)
)
DISTSTYLE EVEN;

-- ----------------------------------------------------------------------------
-- 4. Analytical Views for Dashboard Performance
-- ----------------------------------------------------------------------------

-- View A: Real-Time Overview Aggregations (Today's metrics)
CREATE OR REPLACE VIEW analytics.vw_dashboard_kpis AS
SELECT
    COUNT(*) AS total_records,
    COUNT(CASE WHEN timestamp >= TRUNC(SYSDATE) THEN 1 END) AS records_today,
    COUNT(DISTINCT source) AS total_data_sources,
    ROUND(AVG(processing_time_ms), 1) AS avg_processing_time_ms,
    ROUND(SUM(value), 2) AS total_metric_value,
    ROUND(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
FROM analytics.analytics_records;

-- View B: Source Performance & Volume Comparison
CREATE OR REPLACE VIEW analytics.vw_source_comparison AS
SELECT
    source,
    COUNT(*) AS record_count,
    ROUND(SUM(value), 2) AS total_value,
    ROUND(AVG(value), 2) AS avg_value,
    ROUND(AVG(processing_time_ms), 1) AS avg_latency_ms,
    MAX(timestamp) AS last_record_time,
    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) AS error_count
FROM analytics.analytics_records
GROUP BY source;

-- View C: Category Distribution
CREATE OR REPLACE VIEW analytics.vw_category_distribution AS
SELECT
    category,
    COUNT(*) AS record_count,
    ROUND(SUM(value), 2) AS total_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_share
FROM analytics.analytics_records
GROUP BY category;

-- View D: Hourly Traffic & Throughput Trend (Last 24 Hours)
CREATE OR REPLACE VIEW analytics.vw_hourly_trend AS
SELECT
    DATE_TRUNC('hour', timestamp) AS hour_window,
    source,
    COUNT(*) AS record_count,
    ROUND(SUM(value), 2) AS total_value,
    ROUND(AVG(processing_time_ms), 1) AS avg_processing_time_ms
FROM analytics.analytics_records
WHERE timestamp >= SYSDATE - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', timestamp), source
ORDER BY hour_window DESC;

-- ----------------------------------------------------------------------------
-- 5. Sample Seed Data (For testing or verification in database environments)
-- ----------------------------------------------------------------------------
INSERT INTO analytics.analytics_records (id, timestamp, source, category, value, status, processing_time_ms, metadata)
VALUES
    ('rec-seed-001', SYSDATE - INTERVAL '4 hours', 'Website/Application', 'Page Views', 1420.00, 'SUCCESS', 28, '{"page": "/checkout", "browser": "Chrome"}'),
    ('rec-seed-002', SYSDATE - INTERVAL '3 hours', 'IoT Device', 'Temperature Sensor', 24.50, 'SUCCESS', 15, '{"sensor_id": "SN-9821", "unit": "Celsius"}'),
    ('rec-seed-003', SYSDATE - INTERVAL '2 hours', 'Transaction System', 'Order Payment', 349.99, 'SUCCESS', 45, '{"order_id": "ORD-1102", "currency": "USD"}'),
    ('rec-seed-004', SYSDATE - INTERVAL '90 minutes', 'API', 'Webhook Event', 12.00, 'SUCCESS', 18, '{"partner": "PartnerAPI_A", "endpoint": "/v1/events"}'),
    ('rec-seed-005', SYSDATE - INTERVAL '45 minutes', 'CSV Upload', 'Batch Ingestion', 500.00, 'SUCCESS', 120, '{"filename": "sales_q3.csv", "rows": 500}'),
    ('rec-seed-006', SYSDATE - INTERVAL '20 minutes', 'Website/Application', 'Cart Addition', 89.95, 'SUCCESS', 32, '{"user_id": "usr-883", "sku": "SKU-992"}'),
    ('rec-seed-007', SYSDATE - INTERVAL '10 minutes', 'IoT Device', 'Pressure Gauge', 101.30, 'WARNING', 22, '{"sensor_id": "SN-102", "threshold_exceeded": true}'),
    ('rec-seed-008', SYSDATE - INTERVAL '2 minutes', 'Transaction System', 'Refund Issued', 49.50, 'SUCCESS', 39, '{"refund_id": "RF-402", "reason": "Return"}');

-- Analyze table to update query optimizer statistics
ANALYZE analytics.analytics_records;
