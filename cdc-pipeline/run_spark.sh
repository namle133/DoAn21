#!/bin/bash
# ============================================================
# run_spark.sh
# Script chạy PySpark CDC Consumer với đầy đủ dependencies,
# ghi Delta Lake lên MinIO (S3) để đúng mô hình Data Lake.
# ============================================================

# ─────────────────────────────────────────────────
# Đảm bảo JAVA_HOME được set và tương thích với Spark 3.4.x (Java 8/11/17).
# Tránh dùng Java 21 (gây lỗi NoSuchMethodException DirectByteBuffer).
# Hỗ trợ macOS / Linux / WSL. Ưu tiên JDK 17 > 11 > 8.
# ─────────────────────────────────────────────────

# Lấy major version thật (Java 8 báo "1.8.x" ⇒ 8; Java 17+ báo "17.x" ⇒ 17)
java_major() {
    local java_bin="$1"
    [ -x "$java_bin" ] || return 1
    "$java_bin" -version 2>&1 | awk -F\" '/version/ {
        split($2, v, ".");
        if (v[1] == "1") print v[2]; else print v[1];
        exit
    }'
}

is_compatible_major() {
    case "$1" in 8|11|17) return 0 ;; *) return 1 ;; esac
}

collect_java_homes() {
    # macOS
    if [ -x /usr/libexec/java_home ]; then
        for m in 17 11 8; do
            h=$(/usr/libexec/java_home -v "$m" 2>/dev/null) && [ -d "$h" ] && echo "$h"
        done
    fi
    # macOS filesystem
    for p in /Library/Java/JavaVirtualMachines/*/Contents/Home \
             /opt/homebrew/opt/openjdk* \
             /opt/homebrew/opt/openjdk*/libexec/openjdk.jdk/Contents/Home \
             /usr/local/opt/openjdk* \
             /usr/local/opt/openjdk*/libexec/openjdk.jdk/Contents/Home; do
        [ -d "$p" ] && echo "$p"
    done
    # Linux
    for p in /usr/lib/jvm/* /usr/java/* /opt/java/* /opt/jdk*; do
        [ -d "$p" ] && echo "$p"
    done
    # SDKMAN
    if [ -d "$HOME/.sdkman/candidates/java" ]; then
        for p in "$HOME/.sdkman/candidates/java"/*; do
            [ -d "$p" ] && echo "$p"
        done
    fi
    # jEnv
    if [ -d "$HOME/.jenv/versions" ]; then
        for p in "$HOME/.jenv/versions"/*; do
            [ -d "$p" ] && echo "$p"
        done
    fi
    # `java` trên PATH (fallback cuối)
    if command -v java >/dev/null 2>&1; then
        real=$(readlink -f "$(command -v java)" 2>/dev/null || command -v java)
        h=$(dirname "$(dirname "$real")")
        [ -d "$h" ] && echo "$h"
    fi
}

pick_java_home() {
    local preferred found
    # Ưu tiên: 17 → 11 → 8
    for preferred in 17 11 8; do
        while IFS= read -r home; do
            [ -z "$home" ] && continue
            [ -x "$home/bin/java" ] || continue
            m=$(java_major "$home/bin/java") || continue
            if [ "$m" = "$preferred" ]; then
                echo "$home"
                return 0
            fi
        done < <(collect_java_homes | sort -u)
    done
    return 1
}

# Nếu JAVA_HOME hiện tại đã tương thích thì giữ nguyên
CURRENT_MAJOR=$(java_major "${JAVA_HOME:-}/bin/java" 2>/dev/null)
if ! is_compatible_major "$CURRENT_MAJOR"; then
    CAND=$(pick_java_home) || true
    if [ -n "$CAND" ]; then
        export JAVA_HOME="$CAND"
        export PATH="$JAVA_HOME/bin:$PATH"
    else
        echo "  [WARN] Không tìm thấy JDK 8/11/17 trên máy."
        echo "         Vui lòng cài đặt Java 17 (khuyến nghị) rồi chạy lại."
        echo "         macOS:  brew install openjdk@17"
        echo "         Ubuntu: sudo apt install openjdk-17-jdk"
        echo "         Windows (WSL): sudo apt install openjdk-17-jdk"
    fi
fi

# ─────────────────────────────────────────────────
# Cấu hình Data Lake (MinIO / S3)
# ─────────────────────────────────────────────────
S3_ENDPOINT=${S3_ENDPOINT:-http://localhost:9000}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-minioadmin}
S3_SECRET_KEY=${S3_SECRET_KEY:-minioadmin}

# Mặc định ghi lên MinIO. Muốn chạy local filesystem:
#   DELTA_BASE_PATH=/tmp/delta CHECKPOINT_PATH=/tmp/checkpoint bash run_spark.sh
DELTA_BASE_PATH=${DELTA_BASE_PATH:-s3a://delta-lake/delta}
CHECKPOINT_PATH=${CHECKPOINT_PATH:-s3a://checkpoints/cdc}
TRIGGER_INTERVAL=${TRIGGER_INTERVAL:-10 seconds}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}

# ─────────────────────────────────────────────────
# Thêm --add-opens để tương thích với Java module system
# ─────────────────────────────────────────────────
ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

# ─────────────────────────────────────────────────
# Packages Maven cần thiết
#   - spark-sql-kafka: connector Kafka
#   - delta-core: Delta Lake
#   - hadoop-aws + aws-java-sdk-bundle: S3A để ghi lên MinIO/S3
# Phiên bản hadoop-aws phải khớp với Hadoop của Spark 3.4.1 (Hadoop 3.3.4)
# ─────────────────────────────────────────────────
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
io.delta:delta-core_2.12:2.4.0,\
org.apache.kafka:kafka-clients:3.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262"

echo "============================================"
echo "  Khởi động CDC Consumer"
echo "============================================"
echo "  JAVA_HOME : $JAVA_HOME"
echo "  KAFKA     : $KAFKA_BOOTSTRAP"
echo "  DELTA     : $DELTA_BASE_PATH"
echo "  CHECKPOINT: $CHECKPOINT_PATH"
echo "  S3_ENDPT  : $S3_ENDPOINT"
echo "  TRIGGER   : $TRIGGER_INTERVAL"
echo ""

# Nếu path là local thì đảm bảo thư mục tồn tại (s3a:// thì MinIO đã lo bucket)
case "$DELTA_BASE_PATH" in
    s3a://*|s3://*|hdfs://*) ;;
    *) mkdir -p "$DELTA_BASE_PATH" ;;
esac
case "$CHECKPOINT_PATH" in
    s3a://*|s3://*|hdfs://*) ;;
    *) mkdir -p "$CHECKPOINT_PATH"/{orders,order_products,products} ;;
esac

export KAFKA_BOOTSTRAP DELTA_BASE_PATH CHECKPOINT_PATH TRIGGER_INTERVAL \
       S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY

spark-submit \
    --master "local[4]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.default.parallelism=4 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$S3_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$S3_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
    --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
    --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
    --packages "$PACKAGES" \
    spark/cdc_consumer.py \
    "$@"
