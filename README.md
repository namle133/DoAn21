# CDC Pipeline: MySQL → Debezium → Kafka → Spark → Delta Lake (MinIO / S3)

Hệ thống đồng bộ dữ liệu real-time từ MySQL sang **Delta Lake trên
object storage MinIO (tương thích AWS S3)** sử dụng Change Data Capture
(CDC) với Debezium.

---

## Cấu trúc thư mục

```
cdc-pipeline/
├── docker-compose.yml          ← Toàn bộ infrastructure
├── requirements.txt            ← Python dependencies
├── run_spark.sh                ← Script chạy Spark job
│
├── sql/
│   └── init.sql                ← Schema MySQL + seed data
│
├── debezium/
│   └── register_connector.sh  ← Đăng ký MySQL connector
│
├── simulator/
│   ├── import_data.py          ← Import CSV vào MySQL (chạy 1 lần)
│   └── data_simulator.py       ← Giả lập INSERT/UPDATE/DELETE
│
├── spark/
│   └── cdc_consumer.py         ← PySpark streaming job chính
│
└── analysis/
    └── demo_queries.py         ← Demo và kiểm chứng kết quả
```

---

## Yêu cầu hệ thống

| Thành phần | Phiên bản tối thiểu |
|------------|---------------------|
| Docker Desktop | 4.x |
| Python | 3.9+ |
| Java | **JDK 17** (khuyến nghị) — hoặc 11 / 8. **KHÔNG dùng Java 21+** (Spark 3.4.x chưa hỗ trợ) |
| RAM | 8 GB (khuyến nghị 12 GB) |
| Disk | 15 GB trống |

### Cài Java 17 theo OS

| OS | Lệnh cài |
|----|----------|
| macOS (Homebrew) | `brew install openjdk@17` |
| Ubuntu / Debian / WSL | `sudo apt install openjdk-17-jdk` |
| Fedora / RHEL | `sudo dnf install java-17-openjdk-devel` |
| Arch | `sudo pacman -S jdk17-openjdk` |
| Windows | Tải MSI từ https://adoptium.net/temurin/releases/?version=17 |
| Cross-platform | [SDKMAN](https://sdkman.io/): `sdk install java 17.0.9-tem` |

> **Lưu ý:** Hai script `run_spark.sh` (macOS/Linux/WSL) và
> `run_spark.ps1` (Windows) sẽ **tự động** quét & chọn JDK tương thích
> (ưu tiên 17 > 11 > 8) ngay cả khi `JAVA_HOME` đang trỏ về Java 21.
> Hỗ trợ Homebrew, SDKMAN, jEnv, Adoptium/Temurin, Zulu, Corretto,
> Oracle, apt `update-alternatives`…

---

## Hướng dẫn cài đặt và chạy

### Bước 0: Tải dataset Instacart

1. Vào https://www.kaggle.com/competitions/instacart-market-basket-analysis/data
2. Tải về và giải nén vào thư mục `data/`
3. Cần các file: `orders.csv`, `order_products__prior.csv`, `products.csv`

```
cdc-pipeline/
└── data/
    ├── orders.csv
    ├── order_products__prior.csv
    └── products.csv
```

### Bước 1: Cài Python dependencies

```bash
pip install -r requirements.txt
```

### Bước 2: Khởi động Docker stack

```bash
# Khởi động toàn bộ (MySQL, Zookeeper, Kafka, Debezium)
docker-compose up -d

# Theo dõi logs
docker-compose logs -f

# Chờ tất cả containers healthy (~2 phút)
docker-compose ps
```

Trạng thái mong đợi:
```
cdc_mysql       ... healthy
cdc_zookeeper   ... healthy
cdc_kafka       ... healthy
cdc_debezium    ... healthy
cdc_kafka_ui    ... running
cdc_minio       ... healthy
cdc_minio_setup ... exited (0)   ← container này chạy 1 lần rồi thoát là đúng
```

Service `minio-setup` (image `minio/mc`) tự tạo 2 bucket:
- `delta-lake` — chứa dữ liệu Delta (`/delta/orders`, `/delta/order_products`, `/delta/products`)
- `checkpoints` — chứa Spark Structured Streaming checkpoints

Truy cập **MinIO Console** tại http://localhost:9001 (user/pass:
`minioadmin` / `minioadmin`) để xem bucket & file trực quan.

### Bước 3: Import dữ liệu Instacart vào MySQL

```bash
python simulator/import_data.py --data-dir ./data --limit 50000
```

Kiểm tra:
```bash
# Kết nối MySQL và đếm dòng
docker exec -it cdc_mysql mysql -uroot -proot123 instacart \
  -e "SELECT 'orders', COUNT(*) FROM orders
      UNION ALL SELECT 'order_products', COUNT(*) FROM order_products
      UNION ALL SELECT 'products', COUNT(*) FROM products;"
```

### Bước 4: Đăng ký Debezium Connector

```bash
bash debezium/register_connector.sh
```

Kiểm tra connector đang chạy:
```bash
curl http://localhost:8083/connectors/instacart-mysql-connector/status
```

Kết quả mong đợi:
```json
{
  "name": "instacart-mysql-connector",
  "connector": {"state": "RUNNING"},
  "tasks": [{"state": "RUNNING", "id": 0}]
}
```

Xem Kafka topics được tạo:
```bash
docker exec cdc_kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Topics mong đợi:
- `instacart.instacart.orders`
- `instacart.instacart.order_products`
- `instacart.instacart.products`

### Bước 5: Chạy PySpark CDC Consumer

Mở **terminal mới**:

**macOS / Linux / WSL:**
```bash
chmod +x run_spark.sh
./run_spark.sh
```

**Windows (PowerShell):**
```powershell
.\run_spark.ps1
```

> Cả 2 script đều tự dò `JAVA_HOME` phù hợp (JDK 17/11/8), không cần
> mỗi thành viên phải cấu hình tay.

Mặc định pipeline sẽ ghi Delta Lake lên **MinIO** tại:
- `s3a://delta-lake/delta/{orders,order_products,products}`
- `s3a://checkpoints/cdc/{orders,order_products,products}`

Muốn chạy với filesystem cục bộ (không dùng MinIO):
```bash
DELTA_BASE_PATH=/tmp/delta \
CHECKPOINT_PATH=/tmp/checkpoint \
  bash run_spark.sh
```

### Bước 6: Chạy Data Simulator

Mở **terminal mới**:

```bash
# Giả lập 1.5 sự kiện/giây, chạy liên tục
python simulator/data_simulator.py

# Hoặc giới hạn thời gian (300 giây)
python simulator/data_simulator.py --rate 2 --duration 300
```

### Bước 7: Demo và kiểm chứng

```bash
# Chạy tất cả demo
python analysis/demo_queries.py

# Hoặc từng demo riêng
python analysis/demo_queries.py --demo overview
python analysis/demo_queries.py --demo time-travel
python analysis/demo_queries.py --demo analytics
python analysis/demo_queries.py --demo verify
```

---

## Monitoring

### Kafka UI
Mở trình duyệt: http://localhost:8080

Xem messages trực tiếp:
```bash
docker exec cdc_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic instacart.instacart.orders \
  --from-beginning --max-messages 5
```

### Debezium REST API
```bash
# Danh sách connectors
curl http://localhost:8083/connectors

# Trạng thái connector
curl http://localhost:8083/connectors/instacart-mysql-connector/status

# Xóa và đăng ký lại connector
curl -X DELETE http://localhost:8083/connectors/instacart-mysql-connector
bash debezium/register_connector.sh
```

---

## Kiến trúc luồng dữ liệu

```
MySQL (binlog)
    │
    ▼ đọc binary log
Debezium Connect
    │
    ▼ publish JSON events
Kafka Topics
    instacart.instacart.orders
    instacart.instacart.order_products
    instacart.instacart.products
    │
    ▼ readStream (foreachBatch)
Spark Structured Streaming
    │ parse JSON → phân loại op (c/u/d)
    │ UPSERT: MERGE INTO Delta
    │ DELETE: MERGE + whenMatchedDelete
    ▼ (S3A protocol)
MinIO — Object Storage (tương thích AWS S3)
    s3a://delta-lake/delta/
    ├── orders/          ← Parquet + _delta_log/
    ├── order_products/
    └── products/
    s3a://checkpoints/cdc/   ← Spark Structured Streaming checkpoints
```

### Vì sao dùng MinIO?

MinIO là object storage tương thích **AWS S3 API** 100%, nên code viết
cho MinIO (protocol `s3a://`) chạy thẳng được trên AWS S3 khi triển khai
production, chỉ cần đổi 3 biến môi trường:

| Biến | Local (MinIO) | Production (AWS S3) |
|------|---------------|---------------------|
| `S3_ENDPOINT` | `http://localhost:9000` | để trống (dùng endpoint mặc định) |
| `S3_ACCESS_KEY` | `minioadmin` | AWS access key ID |
| `S3_SECRET_KEY` | `minioadmin` | AWS secret access key |

## Định dạng CDC Event từ Debezium

Sau khi áp dụng `ExtractNewRecordState` transform, mỗi message có dạng:

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

| Field | Giá trị | Ý nghĩa |
|-------|---------|---------|
| `__op` | `c` | CREATE (INSERT) |
| `__op` | `u` | UPDATE |
| `__op` | `d` | DELETE |
| `__op` | `r` | READ (snapshot ban đầu) |
| `__deleted` | `"true"` | Record bị xóa |

---

## Xử lý sự cố thường gặp

**Debezium không kết nối được MySQL:**
```bash
# Kiểm tra MySQL đã bật binlog chưa
docker exec cdc_mysql mysql -uroot -proot123 \
  -e "SHOW VARIABLES LIKE 'log_bin';"
# Kết quả mong đợi: log_bin = ON
```

**Spark không đọc được Kafka:**
```bash
# Kiểm tra Kafka đang chạy
docker exec cdc_kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

**Reset toàn bộ (xóa dữ liệu cũ):**
```bash
docker-compose down -v       # Xóa containers + volumes (bao gồm MinIO data)
rm -rf /tmp/delta /tmp/checkpoint   # nếu có chạy chế độ local
docker-compose up -d
```

**Kiểm tra Delta Lake trên MinIO:**
```bash
# Xem bucket delta-lake qua MinIO client
docker exec cdc_minio mc ls --recursive local/delta-lake

# Hoặc truy cập web console: http://localhost:9001
```

**Thao tác nhanh với bucket (dùng awscli trong Python boto3):**
```bash
python3 -c "
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin')
for obj in s3.list_objects_v2(Bucket='delta-lake').get('Contents', []):
    print(obj['Key'], obj['Size'])
"
```

---

## Dừng hệ thống

```bash
# Dừng Simulator: Ctrl+C
# Dừng Spark: Ctrl+C
# Dừng Docker stack:
docker-compose down
```
