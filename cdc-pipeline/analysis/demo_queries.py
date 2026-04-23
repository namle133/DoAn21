#!/usr/bin/env python3
"""
demo_queries.py
─────────────────────────────────────────────────────────
Demo và kiểm chứng kết quả của CDC Pipeline.
Chạy SAU khi cdc_consumer.py đã xử lý ít nhất 1 batch.

Cách chạy:
    python analysis/demo_queries.py
    python analysis/demo_queries.py --table orders --demo time-travel
"""

import argparse
import os
import re
import sys
import time
import glob
import shutil
import platform
import logging
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


# ─────────────────────────────────────────────────
# Tự dò JDK tương thích Spark 3.4.x (ưu tiên 17, rồi 11, chấp nhận 8).
# Cross-platform: macOS / Linux / Windows. Hỗ trợ Homebrew, SDKMAN,
# Adoptium/Temurin, Zulu, Oracle, Corretto, apt (update-alternatives)…
# ─────────────────────────────────────────────────
# Spark 3.4.x chạy OK với Java 8/11/17. Java 21 gây
# NoSuchMethodException java.nio.DirectByteBuffer.<init>(long,int).
_SPARK_COMPATIBLE_MAJORS = (17, 11, 8)


def _java_major_version(java_home: str):
    """Hỏi chính xác major version qua `java -version`. Trả về int hoặc None."""
    exe = "java.exe" if os.name == "nt" else "java"
    java_bin = os.path.join(java_home, "bin", exe)
    if not os.path.isfile(java_bin):
        return None
    try:
        out = subprocess.run(
            [java_bin, "-version"],
            capture_output=True, text=True, timeout=5,
        )
        text = (out.stderr or "") + (out.stdout or "")
        # Dạng: 'openjdk version "17.0.9"' hoặc '"1.8.0_362"'
        m = re.search(r'version "(\d+)(?:\.(\d+))?', text)
        if not m:
            return None
        major, minor = int(m.group(1)), int(m.group(2) or 0)
        # Java 8 báo là "1.8.x" ⇒ major thật là 8
        return minor if major == 1 else major
    except (OSError, subprocess.SubprocessError):
        return None


def _candidate_java_homes():
    """Liệt kê các thư mục có khả năng là JAVA_HOME, theo từng OS."""
    system = platform.system()
    patterns = []

    if system == "Darwin":
        patterns += [
            "/Library/Java/JavaVirtualMachines/*/Contents/Home",
            "/System/Library/Java/JavaVirtualMachines/*/Contents/Home",
            "/opt/homebrew/opt/openjdk*",
            "/opt/homebrew/opt/openjdk*/libexec/openjdk.jdk/Contents/Home",
            "/opt/homebrew/Cellar/openjdk*/*/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/opt/openjdk*",
            "/usr/local/opt/openjdk*/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/Cellar/openjdk*/*/libexec/openjdk.jdk/Contents/Home",
        ]
    elif system == "Linux":
        patterns += [
            "/usr/lib/jvm/*",
            "/usr/java/*",
            "/opt/java/*",
            "/opt/jdk*",
            "/opt/*/jdk*",
            "/snap/openjdk/current",
        ]
    elif system == "Windows":
        for drive in ("C:", "D:"):
            patterns += [
                drive + r"\Program Files\Java\*",
                drive + r"\Program Files\Eclipse Adoptium\*",
                drive + r"\Program Files\Eclipse Foundation\*",
                drive + r"\Program Files\Amazon Corretto\*",
                drive + r"\Program Files\Microsoft\jdk*",
                drive + r"\Program Files\Zulu\*",
                drive + r"\Program Files\BellSoft\LibericaJDK-*",
                drive + r"\Program Files (x86)\Java\*",
            ]

    # SDKMAN (mọi OS Unix)
    sdkman = os.path.expanduser("~/.sdkman/candidates/java")
    if os.path.isdir(sdkman):
        patterns.append(os.path.join(sdkman, "*"))

    # jEnv (mọi OS Unix)
    jenv = os.path.expanduser("~/.jenv/versions")
    if os.path.isdir(jenv):
        patterns.append(os.path.join(jenv, "*"))

    homes = set()
    for pat in patterns:
        for path in glob.glob(pat):
            homes.add(path)
            # Một số distro đặt JDK trong subdir (ví dụ Contents/Home trên mac,
            # hoặc jdk-17/ bên trong trên Linux). Thử thêm lớp con 1 cấp.
            for sub in ("Contents/Home", "jre", "jdk"):
                cand = os.path.join(path, sub)
                if os.path.isdir(cand):
                    homes.add(cand)
    return list(homes)


def _discover_from_system():
    """Fallback: dùng `java` trên PATH hoặc utility của hệ điều hành."""
    found = []

    # macOS: /usr/libexec/java_home -v <major>
    if platform.system() == "Darwin" and os.path.isfile("/usr/libexec/java_home"):
        for major in _SPARK_COMPATIBLE_MAJORS:
            try:
                out = subprocess.run(
                    ["/usr/libexec/java_home", "-v", str(major)],
                    capture_output=True, text=True, timeout=3,
                )
                home = out.stdout.strip()
                if out.returncode == 0 and home and os.path.isdir(home):
                    found.append(home)
            except (OSError, subprocess.SubprocessError):
                pass

    # Linux: update-alternatives --list java
    if platform.system() == "Linux":
        try:
            out = subprocess.run(
                ["update-alternatives", "--list", "java"],
                capture_output=True, text=True, timeout=3,
            )
            for line in (out.stdout or "").splitlines():
                # /usr/lib/jvm/.../bin/java → lấy parent 2 cấp
                home = os.path.dirname(os.path.dirname(line.strip()))
                if os.path.isdir(home):
                    found.append(home)
        except (OSError, subprocess.SubprocessError):
            pass

    # `java` trên PATH (chuẩn cuối)
    java_on_path = shutil.which("java")
    if java_on_path:
        # Lần ngược: <home>/bin/java → lấy <home>
        try:
            real = os.path.realpath(java_on_path)
            home = os.path.dirname(os.path.dirname(real))
            if os.path.isdir(home):
                found.append(home)
        except OSError:
            pass

    return found


def _ensure_compatible_java():
    """Chọn JAVA_HOME tương thích Spark 3.4.x trên MỌI OS. Im lặng nếu đã đúng."""
    # 1) JAVA_HOME hiện tại đã OK thì giữ nguyên
    current = os.environ.get("JAVA_HOME", "")
    if current:
        major = _java_major_version(current)
        if major in _SPARK_COMPATIBLE_MAJORS:
            logging.info(f"Giữ JAVA_HOME hiện tại (Java {major}): {current}")
            _set_spark_submit_args()
            return

    # 2) Quét các vị trí cài đặt JDK phổ biến
    candidates = _candidate_java_homes() + _discover_from_system()

    # Gom theo major version và chọn preferred (17 > 11 > 8)
    by_major = {}
    for home in candidates:
        major = _java_major_version(home)
        if major in _SPARK_COMPATIBLE_MAJORS:
            by_major.setdefault(major, []).append(home)

    picked_home, picked_major = None, None
    for major in _SPARK_COMPATIBLE_MAJORS:
        if major in by_major:
            picked_home = sorted(by_major[major])[0]
            picked_major = major
            break

    if picked_home:
        os.environ["JAVA_HOME"] = picked_home
        bin_dir = os.path.join(picked_home, "bin")
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
        logging.info(f"Dùng JAVA_HOME (Java {picked_major}): {picked_home}")
    else:
        logging.warning(
            "Không tìm thấy JDK 8/11/17 tương thích Spark 3.4.x. "
            "Nếu gặp lỗi, cài JDK 17 (khuyến nghị) và set biến môi trường JAVA_HOME."
        )

    _set_spark_submit_args()


def _set_spark_submit_args():
    """Thiết lập PYSPARK_SUBMIT_ARGS với --add-opens và các jar cần thiết."""
    add_opens = " ".join([
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ])
    packages = ",".join([
        "io.delta:delta-core_2.12:2.4.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f'--packages {packages} --driver-java-options "{add_opens}" pyspark-shell',
    )


_ensure_compatible_java()


# Cấu hình Data Lake / MinIO
S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
DELTA_BASE    = os.getenv("DELTA_BASE_PATH", "s3a://delta-lake/delta")


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
        .appName("CDC-Demo-Queries")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
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
        .getOrCreate()
    )

def section(title):
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")

def hr():
    print(f"{'─'*60}")


# ─────────────────────────────────────────────────
# Demo 1: Thống kê tổng quan Delta Lake
# ─────────────────────────────────────────────────
def demo_overview(spark):
    section("TỔNG QUAN DELTA LAKE")
    from delta.tables import DeltaTable

    tables = ['orders', 'order_products', 'products']
    for tbl in tables:
        path = f"{DELTA_BASE}/{tbl}"
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"  [SKIP] {tbl}: chưa có Delta table tại {path}")
            continue

        df = spark.read.format("delta").load(path)
        count = df.count()
        delta_tbl = DeltaTable.forPath(spark, path)
        history = delta_tbl.history()
        n_versions = history.count()
        latest_op = history.select("operation").first()[0]

        print(f"\n  Bảng: {tbl}")
        print(f"    Số dòng hiện tại  : {count:>10,}")
        print(f"    Số versions       : {n_versions:>10,}")
        print(f"    Operation mới nhất: {latest_op}")
        print(f"    Cột               : {df.columns}")


# ─────────────────────────────────────────────────
# Demo 2: Xem dữ liệu hiện tại
# ─────────────────────────────────────────────────
def demo_current_data(spark):
    section("DỮ LIỆU HIỆN TẠI — ORDERS")
    path = f"{DELTA_BASE}/orders"
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        print("  Chưa có dữ liệu. Hãy chạy cdc_consumer.py trước.")
        return

    df = spark.read.format("delta").load(path)
    total = df.count()

    print(f"\n  Tổng đơn hàng: {total:,}")
    print(f"\n  10 dòng mẫu:")
    df.orderBy("order_id").show(10, truncate=False)

    print("  Phân bố theo status:")
    df.groupBy("status").count().orderBy("count", ascending=False).show()

    print("  Phân bố theo giờ đặt hàng (top 5):")
    (df.groupBy("order_hour_of_day")
       .count()
       .orderBy("count", ascending=False)
       .limit(5)
       .show())


# ─────────────────────────────────────────────────
# Demo 3: Time Travel — điểm mạnh nhất của Delta Lake
# ─────────────────────────────────────────────────
def demo_time_travel(spark):
    section("TIME TRAVEL — XEM DỮ LIỆU QUÁ KHỨ")
    path = f"{DELTA_BASE}/orders"
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        print("  Chưa có dữ liệu.")
        return

    delta_tbl = DeltaTable.forPath(spark, path)
    history_df = delta_tbl.history()
    n_versions = history_df.count()

    print(f"\n  Lịch sử thay đổi ({n_versions} versions):")
    history_df.select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(20, truncate=False)

    # So sánh số dòng qua các version
    print("\n  So sánh số dòng theo version:")
    print(f"  {'Version':<12} {'Số dòng':>12} {'Thay đổi':>12}")
    hr()
    prev_count = None
    for v in range(min(n_versions, 10)):
        try:
            vdf = (spark.read.format("delta")
                   .option("versionAsOf", v)
                   .load(path))
            count = vdf.count()
            delta_str = ""
            if prev_count is not None:
                diff = count - prev_count
                delta_str = f"+{diff}" if diff >= 0 else str(diff)
            print(f"  v{v:<11} {count:>12,} {delta_str:>12}")
            prev_count = count
        except Exception as e:
            print(f"  v{v:<11} [Không đọc được: {e}]")

    # Đọc version cũ nhất
    print(f"\n  Dữ liệu tại version 0 (snapshot ban đầu, 5 dòng):")
    try:
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
        v0.orderBy("order_id").show(5, truncate=False)
    except Exception as e:
        print(f"  Lỗi: {e}")


# ─────────────────────────────────────────────────
# Demo 4: Kiểm chứng CDC đúng — INSERT/UPDATE/DELETE
# ─────────────────────────────────────────────────
def demo_cdc_verification(spark):
    section("KIỂM CHỨNG CDC — INSERT / UPDATE / DELETE")
    import mysql.connector

    try:
        conn = mysql.connector.connect(
            host='localhost', port=3306,
            user='root', password='root123',
            database='instacart'
        )
        cursor = conn.cursor(dictionary=True)
    except Exception as e:
        print(f"  Không kết nối được MySQL: {e}")
        print("  Bỏ qua demo này.")
        return

    path = f"{DELTA_BASE}/orders"
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, path):
        print("  Chưa có Delta table.")
        conn.close()
        return

    # Lấy một order_id ngẫu nhiên từ MySQL
    cursor.execute("SELECT order_id, status FROM orders LIMIT 1")
    row = cursor.fetchone()
    if not row:
        print("  MySQL không có dữ liệu.")
        conn.close()
        return

    oid = row['order_id']
    print(f"\n  Test với order_id = {oid}")

    # ── Đọc trạng thái hiện tại từ Delta Lake ──
    delta_df = spark.read.format("delta").load(path)
    before = delta_df.filter(f"order_id = {oid}").first()
    print(f"\n  [BEFORE] Trong Delta Lake: {before.asDict() if before else 'Không tồn tại'}")

    # ── Thực hiện UPDATE trên MySQL ──
    new_status = 'cancelled'
    cursor.execute(
        "UPDATE orders SET status = %s WHERE order_id = %s",
        (new_status, oid)
    )
    conn.commit()
    print(f"\n  [ACTION] UPDATE orders SET status='{new_status}' WHERE order_id={oid}")
    print("  Chờ Spark xử lý batch (~15 giây)...")
    time.sleep(17)

    # ── Đọc lại từ Delta Lake ──
    delta_df_after = spark.read.format("delta").load(path)
    after = delta_df_after.filter(f"order_id = {oid}").first()
    print(f"\n  [AFTER]  Trong Delta Lake: {after.asDict() if after else 'Không tồn tại'}")

    if after and after['status'] == new_status:
        print("\n  THÀNH CÔNG! Delta Lake đã phản ánh thay đổi từ MySQL.")
    else:
        print("\n  Chưa đồng bộ. Consumer có thể chưa xử lý batch này.")

    cursor.close()
    conn.close()


# ─────────────────────────────────────────────────
# Demo 5: Analytics trên Delta Lake
# ─────────────────────────────────────────────────
def demo_analytics(spark):
    section("ANALYTICS TRÊN DELTA LAKE")
    orders_path = f"{DELTA_BASE}/orders"
    op_path     = f"{DELTA_BASE}/order_products"

    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, orders_path):
        print("  Chưa có dữ liệu.")
        return

    orders = spark.read.format("delta").load(orders_path)
    orders.createOrReplaceTempView("orders")

    print("\n  1. Top giờ đặt hàng nhiều nhất:")
    spark.sql("""
        SELECT order_hour_of_day as gio,
               COUNT(*) as so_don,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM orders
        GROUP BY order_hour_of_day
        ORDER BY so_don DESC
        LIMIT 10
    """).show()

    print("  2. Phân bố ngày trong tuần:")
    spark.sql("""
        SELECT
            CASE order_dow
                WHEN 0 THEN 'CN' WHEN 1 THEN 'T2' WHEN 2 THEN 'T3'
                WHEN 3 THEN 'T4' WHEN 4 THEN 'T5' WHEN 5 THEN 'T6'
                WHEN 6 THEN 'T7'
            END as ngay,
            COUNT(*) as so_don
        FROM orders
        GROUP BY order_dow
        ORDER BY order_dow
    """).show()

    print("  3. Số đơn hàng theo status:")
    spark.sql("""
        SELECT status, COUNT(*) as so_don
        FROM orders
        GROUP BY status
        ORDER BY so_don DESC
    """).show()

    if DeltaTable.isDeltaTable(spark, op_path):
        op = spark.read.format("delta").load(op_path)
        op.createOrReplaceTempView("order_products")
        print("  4. Top 10 sản phẩm được đặt nhiều nhất:")
        spark.sql("""
            SELECT product_id, COUNT(*) as so_lan_dat
            FROM order_products
            GROUP BY product_id
            ORDER BY so_lan_dat DESC
            LIMIT 10
        """).show()


# ─────────────────────────────────────────────────
# Demo 6: Delta Lake metadata
# ─────────────────────────────────────────────────
def demo_delta_metadata(spark):
    section("DELTA LAKE METADATA & TRANSACTION LOG")
    path = f"{DELTA_BASE}/orders"
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        print("  Chưa có dữ liệu.")
        return

    delta_tbl = DeltaTable.forPath(spark, path)

    print("\n  Detail bảng orders:")
    spark.sql(f"DESCRIBE DETAIL delta.`{path}`").show(truncate=False)

    print("\n  5 operations gần nhất:")
    delta_tbl.history(5).select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    print("\n  File listing (Delta Parquet files):")
    import subprocess
    result = subprocess.run(
        ["find", path, "-name", "*.parquet", "-type", "f"],
        capture_output=True, text=True
    )
    files = result.stdout.strip().split('\n') if result.stdout.strip() else []
    print(f"  Tổng số Parquet files: {len(files)}")
    for f in files[:5]:
        size = os.path.getsize(f) // 1024 if os.path.exists(f) else 0
        print(f"    {os.path.basename(f)}  ({size} KB)")
    if len(files) > 5:
        print(f"    ... và {len(files)-5} files khác")


# ─────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Demo Queries")
    parser.add_argument('--demo', choices=[
        'all', 'overview', 'current', 'time-travel',
        'verify', 'analytics', 'metadata'
    ], default='all', help='Demo nào muốn chạy (default: all)')
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    demos = {
        'overview':    demo_overview,
        'current':     demo_current_data,
        'time-travel': demo_time_travel,
        'verify':      demo_cdc_verification,
        'analytics':   demo_analytics,
        'metadata':    demo_delta_metadata,
    }

    if args.demo == 'all':
        for fn in demos.values():
            fn(spark)
    else:
        demos[args.demo](spark)

    print("\n  Demo hoàn tất!\n")


if __name__ == "__main__":
    main()
