# AWS Cloud Architecture Documentation

## Cloud-Based Data Analytics Dashboard

This document details the complete end-to-end cloud architecture, data processing flow, scalability mechanics, security policies, and deployment blueprints for the **Cloud-Based Data Analytics Dashboard**.

---

## 1. System Architecture Diagram

```mermaid
flowchart TD
    subgraph DataSources["1. Multi-Source Ingestion Layer"]
        DS1["Website / Web App Events"]
        DS2["IoT Sensor Arrays"]
        DS3["Transaction Systems (POS / Stripe)"]
        DS4["External REST APIs / Webhooks"]
        DS5["CSV / Batch File Uploads"]
    end

    subgraph AWS_Kinesis["2. Real-Time Streaming Buffer"]
        KS["Amazon Kinesis Data Streams<br/>(Stream: analytics-data-stream)<br/>• Partition Key: source / device_id<br/>• Shards: Auto-scaled (1-10 shards)"]
    end

    subgraph AWS_Compute["3. Serverless Compute & Transformation"]
        LM["AWS Lambda (dataProcessor)<br/>• Batch Size: 100 records<br/>• Batch Window: 5 seconds<br/>• Schema Validation & Normalization<br/>• Anomaly Detection"]
        S3_DLQ[("Amazon S3 DLQ<br/>(Rejected / Corrupted Records)")]
    end

    subgraph AWS_Storage["4. Cloud Data Warehouse"]
        RS[("Amazon Redshift Cluster<br/>(analyticsdb.analytics_records)<br/>• DISTKEY: source<br/>• SORTKEY: timestamp, category<br/>• Materialized KPI Views")]
        S3_RAW[("Amazon S3 Raw Data Lake<br/>(Optional Historical Archive)")]
    end

    subgraph Backend_Layer["5. API & Live Distribution Layer"]
        API["Node.js Express Backend API<br/>• Dual Mode: Demo & AWS Live<br/>• SSE / WebSocket Stream Server<br/>• Redshift Data API Query Client"]
    end

    subgraph Monitoring_Layer["6. CloudWatch Telemetry & Auto Scaling"]
        CW["Amazon CloudWatch<br/>• IteratorAgeMilliseconds<br/>• Lambda Invocations & Errors<br/>• Redshift Query Runtime<br/>• Auto Scaling Alarms"]
    end

    subgraph Frontend_Layer["7. User Visualization Dashboard"]
        UI["React.js + Vite + Tailwind CSS<br/>• Live KPI Metric Cards<br/>• Real-Time Trend Charts (Chart.js)<br/>• Source Ingestion Simulator<br/>• Reports & CSV Export"]
    end

    %% Pipeline connections
    DS1 -->|PutRecord / PutRecords| KS
    DS2 -->|MQTT / Kinesis Agent| KS
    DS3 -->|SDK Ingestion| KS
    DS4 -->|API Gateway -> Kinesis| KS
    DS5 -->|Direct Upload API| KS

    KS -->|Event Source Mapping| LM
    LM -->|Valid Batch INSERT| RS
    LM -->|Invalid Records| S3_DLQ
    LM -.->|Periodic Raw Dump| S3_RAW

    RS -->|Analytical SQL Queries| API
    API -->|REST API & Live Push| UI
    UI -->|Interactive Test Events| API
    API -.->|Ingest Test Events| KS

    KS -.->|Metrics| CW
    LM -.->|Logs & Metrics| CW
    RS -.->|Performance Metrics| CW
```

---

## 2. Component Details & Design Rationales

### A. Amazon Kinesis Data Streams (Real-Time Ingestion)
- **Role**: Collects and buffers real-time events arriving simultaneously from multiple disparate sources.
- **Partition Key Strategy**: 
  - Uses `source` or `device_id` as the partition key.
  - Ensures uniform record distribution across Kinesis shards while preserving per-device or per-source ordering.
- **Retention Period**: Default 24 hours (can be extended to 7 days for historical replay).
- **Scalability**:
  - Each shard supports **1 MB/sec (or 1,000 records/sec) write** and **2 MB/sec read**.
  - Dynamic scaling managed via AWS Application Auto Scaling based on `IncomingBytes` and `IncomingRecords` metrics.

### B. AWS Lambda (Serverless Stream Processing)
- **Role**: Lightweight, event-driven data cleaning, validation, and transformation layer.
- **Trigger**: Kinesis Event Source Mapping (`StartingPosition: LATEST`).
- **Batching Parameters**:
  - **Batch Size**: 100 records (balances Lambda invocation cost and real-time delivery latency).
  - **Batch Window**: 5 seconds (ensures batches fill up even during lower-volume periods).
- **Transformation Steps**:
  1. Base64 payload decoding.
  2. JSON parsing with schema integrity checks.
  3. Timestamp normalization to UTC ISO 8601 (`YYYY-MM-DDTHH:mm:ss.sssZ`).
  4. Numerical value sanitization and rounding to 2 decimal places.
  5. Dead Letter Queue (DLQ) separation: corrupt payloads are forwarded to an S3 bucket or audit table without terminating the batch.

### C. Amazon Redshift (Cloud Analytical Data Warehouse)
- **Role**: Columnar analytical storage supporting sub-second aggregation across millions of records.
- **Schema & Optimization**:
  - **DISTSTYLE KEY (source)**: Co-locates rows with the same source on the same compute node slice. This drastically speeds up aggregations grouped by data source.
  - **COMPOUND SORTKEY (timestamp, category)**: Leverages 1 MB columnar zone maps (Min/Max values). Queries filtering on specific date ranges or categories skip 90%+ of irrelevant disk blocks.
  - **Column Encodings**: `az64` for numerical values and timestamps (best performance and compression in Redshift), `zstd` for variable length JSON metadata, `bytedict` for enumerated categories.
  - **Materialized Views**: `vw_dashboard_kpis` and `vw_source_comparison` provide pre-aggregated figures for instantaneous dashboard rendering.

### D. Amazon S3 (Data Lake & Dead-Letter Backup)
- **Role**:
  - **Raw Staging / Long-Term Cold Storage**: Cost-effective storage for raw JSON events.
  - **Dead Letter Queue (DLQ)**: Stores records that failed Lambda validation for offline auditing and root-cause analysis.

### E. Amazon CloudWatch (Observability & Alarms)
- **Metrics Tracked**:
  - `GetRecords.IteratorAgeMilliseconds`: Indicates if Lambda is lagging behind the Kinesis stream.
  - `IncomingRecords` & `IncomingBytes`: Input stream volume.
  - `Lambda Errors` & `Throttles`: Processing health.
  - `Redshift QueryRuntime`: Analytical performance.
- **Alarm Actions**: Automatically triggers scale-out when `IteratorAgeMilliseconds > 30000ms`.

---

## 3. Scalability & Resilience Strategy

| Level | Scaling Mechanism | Trigger / Strategy |
| :--- | :--- | :--- |
| **Ingestion** | Kinesis Shard Auto-Splitting | Scaled when write throughput exceeds 80% (800 KB/s per shard). |
| **Compute** | AWS Lambda Concurrency | Automatically scales up to 1 concurrent Lambda execution per Kinesis shard. |
| **Storage** | Redshift Concurrency Scaling & Elastic Resize | Dynamically adds transient clusters during heavy analytical workloads. |
| **API Backend** | AWS Auto Scaling Group (EC2 / ECS Fargate) | Scales out based on Target Tracking Policy (Average CPU > 70%). |
| **Fault Tolerance** | Automatic DLQ Routing | Malformed records do not block stream processing; they are safely quarantined. |

---

## 4. Transitioning: Demo Mode vs. Real AWS Mode

The application is engineered with an intelligent **Dual Mode** abstraction layer:

```
                  ┌───────────────────────┐
                  │ Frontend / Dashboard  │
                  └───────────┬───────────┘
                              │
                  ┌───────────▼───────────┐
                  │    Express Backend    │
                  └───────────┬───────────┘
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
    [DEMO MODE: ACTIVE]               [AWS MODE: ACTIVE]
   • In-Memory Ring Buffer           • Amazon Kinesis SDK Client
   • Multi-Source Event Simulator    • Amazon Redshift Data Client
   • Synthetic Latencies (15-40ms)   • Amazon CloudWatch Metrics Client
   • Simulated Sensor & Sales Data   • S3 Storage & DLQ Sync
```

- When `DEMO_MODE=true` (or when AWS credentials are absent), the system operates entirely offline using realistic multi-source synthetic event generators.
- When `DEMO_MODE=false` and valid AWS credentials exist in `.env`, the system automatically routes all read and write queries to your real AWS cloud services.

---

## 5. Step-by-Step AWS Setup Guide

### Step 1: Create Amazon Kinesis Data Stream
```bash
aws kinesis create-stream \
  --stream-name analytics-data-stream \
  --shard-count 2 \
  --region us-east-1
```

### Step 2: Create Amazon S3 Buckets
```bash
# Bucket for Raw Data & DLQ
aws s3api create-bucket \
  --bucket analytics-raw-data-bucket-prod \
  --region us-east-1

aws s3api create-bucket \
  --bucket analytics-dlq-failed-records-prod \
  --region us-east-1
```

### Step 3: Create Amazon Redshift Serverless or Provisioned Cluster
1. In the AWS Management Console, open **Amazon Redshift**.
2. Create a Serverless Workgroup (e.g. `analytics-workgroup`) or a DC2.large provisioned cluster.
3. Database Name: `analyticsdb`, Master Username: `awsuser`.
4. Open the **Query Editor v2** and execute the script in [`database/schema.sql`](../database/schema.sql).

### Step 4: Deploy the AWS Lambda Function
1. In AWS Lambda Console, choose **Create function** > **Author from scratch**.
2. Function name: `analytics-data-processor`. Runtime: `Node.js 20.x`.
3. Execution role: Attach policy with permissions for `AWSLambdaKinesisExecutionRole`, `AmazonRedshiftDataFullAccess`, and `AmazonS3FullAccess`.
4. Upload `lambda/dataProcessor/index.js` or zip package.
5. Add Trigger: Select **Kinesis**, select `analytics-data-stream`, Batch size `100`, Batch window `5` seconds.
6. Configure Environment Variables:
   - `REDSHIFT_DATABASE=analyticsdb`
   - `REDSHIFT_WORKGROUP_NAME=analytics-workgroup`
   - `S3_DLQ_BUCKET=analytics-dlq-failed-records-prod`

### Step 5: Configure `.env` in Backend
Copy `.env.example` to `.env` and fill in your AWS keys:
```env
PORT=5000
DEMO_MODE=false
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
KINESIS_STREAM_NAME=analytics-data-stream
REDSHIFT_DATABASE=analyticsdb
REDSHIFT_WORKGROUP_NAME=analytics-workgroup
```
