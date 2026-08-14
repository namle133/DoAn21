# CDC Pipeline: MySQL → Debezium → Kafka → Spark → Delta Lake (MinIO / S3)

A real-time data synchronization pipeline that streams data from **MySQL** to **Delta Lake on MinIO object storage (AWS S3-compatible)** using **Change Data Capture (CDC)** with Debezium.

---

## Project Structure

```text
cdc-pipeline/
├── docker-compose.yml          ← Complete infrastructure stack
├── requirements.txt            ← Python dependencies
├── run_spark.sh                ← Script for running the Spark job
│
├── sql/
│   └── init.sql                ← MySQL schema and seed data
│
├── debezium/
│   └── register_connector.sh   ← Registers the MySQL connector
│
├── simulator/
│   ├── import_data.py          ← Imports CSV data into MySQL (run once)
│   └── data_simulator.py       ← Simulates INSERT/UPDATE/DELETE operations
│
├── spark/
│   └── cdc_consumer.py         ← Main PySpark streaming job
│
└── analysis/
    └── demo_queries.py         ← Demo queries and result verification
```

---

## System Requirements

| Component | Minimum Version |
|---|---|
| Docker Desktop | 4.x |
| Python | 3.9+ |
| Java | **JDK 17** recommended, or JDK 11 / 8. **Do NOT use Java 21+** because Spark 3.4.x does not support it |
| RAM | 8 GB (12 GB recommended) |
| Disk | 15 GB of free space |

### Installing Java 17

| OS | Installation Command |
|---|---|
| macOS (Homebrew) | `brew install openjdk@17` |
| Ubuntu / Debian / WSL | `sudo apt install openjdk-17-jdk` |
| Fedora / RHEL | `sudo dnf install java-17-openjdk-devel` |
| Arch Linux | `sudo pacman -S jdk17-openjdk` |
| Windows | Download the MSI installer from Adoptium Temurin |
| Cross-platform | SDKMAN: `sdk install java 17.0.9-tem` |

> **Note:** Both `run_spark.sh` (macOS/Linux/WSL) and `run_spark.ps1`
> (Windows) automatically detect and select a compatible JDK,
> prioritizing JDK 17 → 11 → 8, even when `JAVA_HOME` currently points
> to Java 21.
>
> The scripts support Homebrew, SDKMAN, jEnv, Adoptium/Temurin, Zulu,
> Corretto, Oracle, apt `update-alternatives`, and other common Java
> installations.

---

## Setup and Running the Pipeline

### Step 0: Download the Instacart Dataset

1. Go to the **Instacart Market Basket Analysis** dataset on Kaggle.
2. Download and extract the dataset into the `data/` directory.
3. The following files are required:

```text
orders.csv
order_products__prior.csv
products.csv
```

The directory should look like this:

```text
cdc-pipeline/
└── data/
    ├── orders.csv
    ├── order_products__prior.csv
    └── products.csv
```

### Step 1: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Start the Docker Stack

```bash
# Start the entire stack:
# MySQL, Zookeeper, Kafka, Debezium, Kafka UI, and MinIO
docker-compose up -d

# Follow container logs
docker-compose logs -f

# Wait until all containers become healthy (~2 minutes)
docker-compose ps
```

Expected status:

```text
cdc_mysql       ... healthy
cdc_zookeeper   ... healthy
cdc_kafka       ... healthy
cdc_debezium    ... healthy
cdc_kafka_ui    ... running
cdc_minio       ... healthy
cdc_minio_setup ... exited (0)
```

The `cdc_minio_setup` container is expected to run once and exit with
status code `0`.

The `minio-setup` service, which uses the `minio/mc` image,
automatically creates two buckets:

- `delta-lake` — stores Delta Lake data under
  `/delta/orders`, `/delta/order_products`, and `/delta/products`
- `checkpoints` — stores Spark Structured Streaming checkpoints

The **MinIO Console** is available at:

```text
http://localhost:9001
```

Default credentials:

```text
Username: minioadmin
Password: minioadmin
```

The console can be used to inspect buckets and stored objects.

---

### Step 3: Import the Instacart Dataset into MySQL

```bash
python simulator/import_data.py --data-dir ./data --limit 50000
```

Verify the imported data:

```bash
docker exec -it cdc_mysql mysql -uroot -proot123 instacart \
  -e "SELECT 'orders', COUNT(*) FROM orders
      UNION ALL SELECT 'order_products', COUNT(*) FROM order_products
      UNION ALL SELECT 'products', COUNT(*) FROM products;"
```

---

### Step 4: Register the Debezium Connector

```bash
bash debezium/register_connector.sh
```

Check whether the connector is running:

```bash
curl http://localhost:8083/connectors/instacart-mysql-connector/status
```

Expected response:

```json
{
  "name": "instacart-mysql-connector",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "state": "RUNNING",
      "id": 0
    }
  ]
}
```

List the Kafka topics created by Debezium:

```bash
docker exec cdc_kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Expected topics:

```text
instacart.instacart.orders
instacart.instacart.order_products
instacart.instacart.products
```

---

### Step 5: Start the PySpark CDC Consumer

Open a **new terminal**.

#### macOS / Linux / WSL

```bash
chmod +x run_spark.sh
./run_spark.sh
```

#### Windows (PowerShell)

```powershell
.\run_spark.ps1
```

> Both scripts automatically detect a compatible `JAVA_HOME`
> (JDK 17/11/8), so individual users do not need to configure it
> manually.

By default, the pipeline writes Delta Lake data to **MinIO**:

```text
s3a://delta-lake/delta/orders
s3a://delta-lake/delta/order_products
s3a://delta-lake/delta/products
```

Spark Structured Streaming checkpoints are stored at:

```text
s3a://checkpoints/cdc/orders
s3a://checkpoints/cdc/order_products
s3a://checkpoints/cdc/products
```

To run the pipeline using the local filesystem instead of MinIO:

```bash
DELTA_BASE_PATH=/tmp/delta \
CHECKPOINT_PATH=/tmp/checkpoint \
  bash run_spark.sh
```

---

### Step 6: Start the Data Simulator

Open another **new terminal**:

```bash
# Simulate approximately 1.5 events per second continuously
python simulator/data_simulator.py
```

Or run the simulator for a limited duration:

```bash
python simulator/data_simulator.py --rate 2 --duration 300
```

The simulator generates database changes such as:

- INSERT
- UPDATE
- DELETE

These changes are captured from the MySQL binary log by Debezium and
propagated through the CDC pipeline.

---

### Step 7: Run Demo Queries and Verify the Results

Run all demonstrations:

```bash
python analysis/demo_queries.py
```

Or run individual demos:

```bash
python analysis/demo_queries.py --demo overview
python analysis/demo_queries.py --demo time-travel
python analysis/demo_queries.py --demo analytics
python analysis/demo_queries.py --demo verify
```

---

## Monitoring

### Kafka UI

Open:

```text
http://localhost:8080
```

You can also inspect Kafka messages directly from the command line:

```bash
docker exec cdc_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic instacart.instacart.orders \
  --from-beginning --max-messages 5
```

### Debezium REST API

List registered connectors:

```bash
curl http://localhost:8083/connectors
```

Check the connector status:

```bash
curl http://localhost:8083/connectors/instacart-mysql-connector/status
```

Delete and re-register the connector:

```bash
curl -X DELETE http://localhost:8083/connectors/instacart-mysql-connector

bash debezium/register_connector.sh
```

---

## Data Flow Architecture

```text
MySQL (binlog)
    │
    │ Read binary log
    ▼
Debezium Connect
    │
    │ Publish JSON CDC events
    ▼
Kafka Topics
    │
    ├── instacart.instacart.orders
    ├── instacart.instacart.order_products
    └── instacart.instacart.products
    │
    │ readStream + foreachBatch
    ▼
Spark Structured Streaming
    │
    │ Parse JSON
    │
    │ Classify CDC operations (c/u/d)
    │
    ├── UPSERT → MERGE INTO Delta
    │
    └── DELETE → MERGE + whenMatchedDelete
    │
    │ S3A protocol
    ▼
MinIO — S3-Compatible Object Storage
    │
    ├── s3a://delta-lake/delta/
    │   ├── orders/
    │   │   ├── Parquet files
    │   │   └── _delta_log/
    │   ├── order_products/
    │   └── products/
    │
    └── s3a://checkpoints/cdc/
        └── Spark Structured Streaming checkpoints
```

---

## Why MinIO?

MinIO provides object storage compatible with the **AWS S3 API**.

Because the pipeline uses the `s3a://` protocol, code developed against
MinIO can be migrated to AWS S3 for production deployment with minimal
changes.

Only three environment variables need to be changed:

| Variable | Local (MinIO) | Production (AWS S3) |
|---|---|---|
| `S3_ENDPOINT` | `http://localhost:9000` | Leave empty to use the default AWS endpoint |
| `S3_ACCESS_KEY` | `minioadmin` | AWS Access Key ID |
| `S3_SECRET_KEY` | `minioadmin` | AWS Secret Access Key |

This allows the same Spark and Delta Lake architecture to be tested
locally before being deployed to cloud object storage.

---

## Debezium CDC Event Format

After applying the `ExtractNewRecordState` transform, each Kafka message
has the following structure:

```json
{
  "order_id": 1234,
  "user_id": 5678,
  "status": "completed",
  "order_hour_of_day": 14,
  "__op": "u",
  "__table": "orders",
  "__source_ts_ms": 1703123456789,
  "__deleted": "false"
}
```

### CDC Operation Types

| Field | Value | Meaning |
|---|---|---|
| `__op` | `c` | CREATE (INSERT) |
| `__op` | `u` | UPDATE |
| `__op` | `d` | DELETE |
| `__op` | `r` | READ (initial snapshot) |
| `__deleted` | `"true"` | The record has been deleted |

---

## Troubleshooting

### Debezium Cannot Connect to MySQL

Check whether MySQL binary logging is enabled:

```bash
docker exec cdc_mysql mysql -uroot -proot123 \
  -e "SHOW VARIABLES LIKE 'log_bin';"
```

Expected result:

```text
log_bin = ON
```

---

### Spark Cannot Read from Kafka

Check whether Kafka is running correctly:

```bash
docker exec cdc_kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

---

### Reset the Entire Environment

To remove all existing containers, volumes, and MinIO data:

```bash
docker-compose down -v
```

If the pipeline was previously run using local filesystem storage:

```bash
rm -rf /tmp/delta /tmp/checkpoint
```

Restart the stack:

```bash
docker-compose up -d
```

---

### Inspect Delta Lake Data on MinIO

List the contents of the `delta-lake` bucket:

```bash
docker exec cdc_minio mc ls --recursive local/delta-lake
```

Alternatively, open the MinIO web console:

```text
http://localhost:9001
```

---

### Access MinIO with boto3

You can inspect the bucket programmatically using Python and `boto3`:

```bash
python3 -c "
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

for obj in s3.list_objects_v2(
    Bucket='delta-lake'
).get('Contents', []):
    print(obj['Key'], obj['Size'])
"
```

---

## Stopping the Pipeline

Stop the Data Simulator:

```text
Ctrl+C
```

Stop Spark:

```text
Ctrl+C
```

Stop the Docker stack:

```bash
docker-compose down
```

---

## Technology Stack

- **Database:** MySQL
- **Change Data Capture:** Debezium
- **Message Streaming:** Apache Kafka
- **Stream Processing:** Apache Spark / PySpark Structured Streaming
- **Data Lake:** Delta Lake
- **Object Storage:** MinIO / AWS S3-compatible storage
- **Containerization:** Docker / Docker Compose
- **Programming:** Python
- **Dataset:** Instacart Market Basket Analysis

---

## Pipeline Summary

```text
MySQL
  ↓ CDC / Binlog
Debezium
  ↓
Kafka
  ↓
Spark Structured Streaming
  ↓
Delta Lake
  ↓
MinIO / AWS S3
```

The pipeline demonstrates an end-to-end **real-time CDC and data lake
architecture**, capturing changes from a transactional MySQL database,
streaming them through Kafka, processing them with Spark Structured
Streaming, and persisting the resulting state in Delta Lake on
S3-compatible object storage.

## 👨‍💻 Author

**Le Thanh Nam**

Bachelor of Science in Information Technology  
VNUHCM - University of Information Technology (UIT)
