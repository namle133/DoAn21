#!/usr/bin/env python3
"""
cdc_consumer.py
─────────────────────────────────────────────────────────
PySpark Structured Streaming: đọc CDC events từ Kafka,
áp dụng INSERT / UPDATE / DELETE vào Delta Lake.

Cách chạy:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
io.delta:delta-core_2.12:2.4.0 \
      spark/cdc_consumer.py

Hoặc dùng script run.sh đã cấu hình sẵn.
"""

import json
import os
import sys
import time
import signal
import logging
import threading
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType, FloatType, TimestampType
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("CDC-Consumer")

# ─────────────────────────────────────────────────
# Cấu hình
# ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
# Mặc định ghi Delta Lake lên MinIO (S3). Đổi sang local bằng env DELTA_BASE_PATH=/tmp/delta.
DELTA_BASE_PATH  = os.getenv("DELTA_BASE_PATH",  "s3a://delta-lake/delta")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH",  "s3a://checkpoints/cdc")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# Cấu hình S3A cho MinIO (tương thích AWS S3)
S3_ENDPOINT      = os.getenv("S3_ENDPOINT",      "http://localhost:9000")
S3_ACCESS_KEY    = os.getenv("S3_ACCESS_KEY",    "minioadmin")
S3_SECRET_KEY    = os.getenv("S3_SECRET_KEY",    "minioadmin")

KAFKA_TOPICS = {
    "orders":         "instacart.instacart.orders",
    "order_products": "instacart.instacart.order_products",
    "products":       "instacart.instacart.products",
}

# ─────────────────────────────────────────────────
# Schema định nghĩa cho từng bảng
# (Debezium với ExtractNewRecordState: payload đã được flatten)
# ─────────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id",           IntegerType(), True),
    StructField("user_id",            IntegerType(), True),
    StructField("eval_set",           StringType(),  True),
    StructField("order_number",       IntegerType(), True),
    StructField("order_dow",          IntegerType(), True),
    StructField("order_hour_of_day",  IntegerType(), True),
    StructField("days_since_prior",   FloatType(),   True),
    StructField("status",             StringType(),  True),
    StructField("created_at",         StringType(),  True),
    StructField("updated_at",         StringType(),  True),
    # Fields thêm bởi Debezium transform
    StructField("__op",               StringType(),  True),
    StructField("__table",            StringType(),  True),
    StructField("__source_ts_ms",     LongType(),    True),
    StructField("__deleted",          StringType(),  True),
])

ORDER_PRODUCTS_SCHEMA = StructType([
    StructField("id",                 LongType(),    True),
    StructField("order_id",           IntegerType(), True),
    StructField("product_id",         IntegerType(), True),
    StructField("add_to_cart_order",  IntegerType(), True),
    StructField("reordered",          IntegerType(), True),
    StructField("created_at",         StringType(),  True),
    StructField("__op",               StringType(),  True),
    StructField("__deleted",          StringType(),  True),
    StructField("__source_ts_ms",     LongType(),    True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",         IntegerType(), True),
    StructField("product_name",       StringType(),  True),
    StructField("aisle_id",           IntegerType(), True),
    StructField("department_id",      IntegerType(), True),
    StructField("__op",               StringType(),  True),
    StructField("__deleted",          StringType(),  True),
    StructField("__source_ts_ms",     LongType(),    True),
])


# ─────────────────────────────────────────────────
# Khởi tạo SparkSession
# ─────────────────────────────────────────────────
def create_spark_session():
    log.info("Khởi tạo SparkSession...")
    builder = (SparkSession.builder
        .appName("CDC-Instacart-Pipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation",
                CHECKPOINT_PATH)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.memory", "2g")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession sẵn sàng.")
    return spark


# ─────────────────────────────────────────────────
# Đọc stream từ Kafka
# ─────────────────────────────────────────────────
def read_kafka_stream(spark, topic_name):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
        .select(F.col("value").cast("string").alias("json_value"),
                F.col("timestamp").alias("kafka_ts"))
    )


# ─────────────────────────────────────────────────
# Logic xử lý batch: MERGE INTO Delta Lake
# ─────────────────────────────────────────────────
def make_batch_processor(table_name, delta_path, pk_col, schema):
    """
    Factory tạo foreachBatch function cho từng bảng.
    Xử lý:
      - __deleted = 'true'  → DELETE khỏi Delta Lake
      - __op = 'c' / 'u'    → MERGE (upsert) vào Delta Lake
    """
    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        start_ts = datetime.now()
        total_rows = batch_df.count()

        # ── Parse JSON ──────────────────────────────────
        parsed = (batch_df
            .select(F.from_json(F.col("json_value"), schema).alias("data"),
                    F.col("kafka_ts"))
            .select("data.*", "kafka_ts")
            # Loại bỏ records null (Tombstone messages)
            .filter(F.col("__op").isNotNull() | F.col("__deleted").isNotNull())
        )

        if parsed.isEmpty():
            return

        # ── Tách DELETE và UPSERT ────────────────────────
        deletes = parsed.filter(
            (F.col("__deleted") == "true") | (F.col("__op") == "d")
        )
        upserts = parsed.filter(
            (F.col("__deleted").isNull() | (F.col("__deleted") != "true")) &
            (F.col("__op").isin("c", "u", "r"))
        )

        # Bỏ các metadata columns trước khi ghi
        meta_cols = ["__op", "__table", "__source_ts_ms", "__deleted", "kafka_ts"]
        data_cols = [c for c in parsed.columns if c not in meta_cols]

        n_upsert = upserts.count()
        n_delete = deletes.count()

        log.info(f"[{table_name}] batch={batch_id} | "
                 f"upsert={n_upsert} | delete={n_delete}")

        # ── UPSERT (INSERT + UPDATE) ──────────────────────
        if n_upsert > 0:
            upsert_data = upserts.select(data_cols)

            if DeltaTable.isDeltaTable(spark_ref[0], delta_path):
                delta_tbl = DeltaTable.forPath(spark_ref[0], delta_path)
                (delta_tbl.alias("target")
                    .merge(
                        upsert_data.alias("source"),
                        f"target.{pk_col} = source.{pk_col}"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                log.info(f"  => MERGE {n_upsert} rows vào {table_name}")
            else:
                # Lần đầu tiên: tạo Delta table
                (upsert_data.write
                    .format("delta")
                    .mode("overwrite")
                    .save(delta_path)
                )
                log.info(f"  => Khởi tạo Delta table: {table_name} ({n_upsert} rows)")

        # ── DELETE ────────────────────────────────────────
        if n_delete > 0 and DeltaTable.isDeltaTable(spark_ref[0], delta_path):
            delete_ids = deletes.select(F.col(pk_col)).distinct()
            delta_tbl = DeltaTable.forPath(spark_ref[0], delta_path)
            (delta_tbl.alias("target")
                .merge(
                    delete_ids.alias("source"),
                    f"target.{pk_col} = source.{pk_col}"
                )
                .whenMatchedDelete()
                .execute()
            )
            log.info(f"  => DELETE {n_delete} rows khỏi {table_name}")

        elapsed_ms = (datetime.now() - start_ts).total_seconds() * 1000
        log.info(f"  => Batch {batch_id} xử lý xong ({elapsed_ms:.0f}ms)")

    return process_batch


# Biến toàn cục để processor closure có thể truy cập spark
spark_ref = [None]


# ─────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark_ref[0] = spark

    log.info("=" * 55)
    log.info("  CDC Pipeline: Kafka → Delta Lake")
    log.info("=" * 55)
    log.info(f"  Kafka    : {KAFKA_BOOTSTRAP}")
    log.info(f"  Delta    : {DELTA_BASE_PATH}")
    log.info(f"  Trigger  : every {TRIGGER_INTERVAL}")
    log.info("=" * 55)

    # Cấu hình từng bảng cần theo dõi
    table_configs = [
        {
            "name":       "orders",
            "topic":      KAFKA_TOPICS["orders"],
            "delta_path": f"{DELTA_BASE_PATH}/orders",
            "pk_col":     "order_id",
            "schema":     ORDERS_SCHEMA,
            "checkpoint": f"{CHECKPOINT_PATH}/orders",
        },
        {
            "name":       "order_products",
            "topic":      KAFKA_TOPICS["order_products"],
            "delta_path": f"{DELTA_BASE_PATH}/order_products",
            "pk_col":     "id",
            "schema":     ORDER_PRODUCTS_SCHEMA,
            "checkpoint": f"{CHECKPOINT_PATH}/order_products",
        },
        {
            "name":       "products",
            "topic":      KAFKA_TOPICS["products"],
            "delta_path": f"{DELTA_BASE_PATH}/products",
            "pk_col":     "product_id",
            "schema":     PRODUCTS_SCHEMA,
            "checkpoint": f"{CHECKPOINT_PATH}/products",
        },
    ]

    queries = []
    for cfg in table_configs:
        log.info(f"Khởi động stream: {cfg['name']} ← {cfg['topic']}")

        raw_stream = read_kafka_stream(spark, cfg["topic"])
        processor = make_batch_processor(
            cfg["name"], cfg["delta_path"], cfg["pk_col"], cfg["schema"]
        )

        query = (raw_stream.writeStream
            .foreachBatch(processor)
            .option("checkpointLocation", cfg["checkpoint"])
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(query)
        log.info(f"  => Stream '{cfg['name']}' đã start (queryId={query.id})")

    log.info("\nTất cả streams đã chạy. Nhấn Ctrl+C để dừng.\n")

    # Dùng Event để Ctrl+C cắt time.sleep() tức thì, giúp shutdown graceful.
    # Ghi đè signal handler của PySpark (vốn stop SparkContext trước khi
    # Python code kịp q.stop() → gây RejectedExecutionException + log bẩn).
    stop_event = threading.Event()

    def _graceful_shutdown(signum, frame):
        if not stop_event.is_set():
            log.info("Nhận tín hiệu dừng (Ctrl+C). Đang dừng các streams...")
            stop_event.set()

    signal.signal(signal.SIGINT,  _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    try:
        while not stop_event.is_set() and all(q.isActive for q in queries):
            if stop_event.wait(timeout=30):
                break
            log.info("─── Trạng thái streams ───")
            for q in queries:
                progress = q.lastProgress
                if progress:
                    rows = progress.get("numInputRows", 0)
                    log.info(f"  {q.name or q.id}: {rows} rows/trigger")
    except KeyboardInterrupt:
        stop_event.set()

    # Dừng từng query. Bọc try/except để lỗi shutdown không làm bẩn log.
    for q in queries:
        try:
            if q.isActive:
                q.stop()
        except Exception as e:  # noqa: BLE001
            log.debug(f"Bỏ qua lỗi stop query: {e}")

    # Chờ queries thoát hẳn rồi mới stop SparkSession
    for q in queries:
        try:
            q.awaitTermination(timeout=15)
        except Exception:
            pass

    try:
        spark.stop()
    except Exception:
        pass

    log.info("CDC Consumer đã dừng.")


if __name__ == "__main__":
    main()
