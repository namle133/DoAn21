# ============================================================
# run_spark.ps1 (Windows PowerShell)
# Script chạy PySpark CDC Consumer trên Windows.
# Tự dò JDK 17/11/8 tương thích Spark 3.4.x, bỏ qua Java 21+.
# ============================================================

$ErrorActionPreference = "Stop"

function Get-JavaMajor($javaExe) {
    if (-not (Test-Path $javaExe)) { return $null }
    try {
        $output = & $javaExe -version 2>&1 | Out-String
        if ($output -match 'version "(\d+)(?:\.(\d+))?') {
            $major = [int]$Matches[1]
            $minor = if ($Matches[2]) { [int]$Matches[2] } else { 0 }
            if ($major -eq 1) { return $minor } else { return $major }
        }
    } catch {}
    return $null
}

function Get-CandidateJavaHomes {
    $roots = @(
        "C:\Program Files\Java",
        "C:\Program Files\Eclipse Adoptium",
        "C:\Program Files\Eclipse Foundation",
        "C:\Program Files\Amazon Corretto",
        "C:\Program Files\Microsoft",
        "C:\Program Files\Zulu",
        "C:\Program Files\BellSoft",
        "C:\Program Files (x86)\Java"
    )
    $homes = @()
    foreach ($root in $roots) {
        if (Test-Path $root) {
            Get-ChildItem $root -Directory -ErrorAction SilentlyContinue |
                ForEach-Object { $homes += $_.FullName }
        }
    }
    # SDKMAN for Windows (nếu có)
    $sdkman = Join-Path $HOME ".sdkman\candidates\java"
    if (Test-Path $sdkman) {
        Get-ChildItem $sdkman -Directory | ForEach-Object { $homes += $_.FullName }
    }
    # java.exe trên PATH
    $javaOnPath = (Get-Command java -ErrorAction SilentlyContinue)
    if ($javaOnPath) {
        $homes += (Split-Path (Split-Path $javaOnPath.Source))
    }
    return $homes | Select-Object -Unique
}

function Select-CompatibleJavaHome {
    $homes = Get-CandidateJavaHomes
    foreach ($preferred in 17, 11, 8) {
        foreach ($home in $homes) {
            $exe = Join-Path $home "bin\java.exe"
            $m = Get-JavaMajor $exe
            if ($m -eq $preferred) { return $home }
        }
    }
    return $null
}

# ─────── Chọn JAVA_HOME ───────
$currentMajor = if ($env:JAVA_HOME) {
    Get-JavaMajor (Join-Path $env:JAVA_HOME "bin\java.exe")
} else { $null }

if ($currentMajor -notin 8, 11, 17) {
    $pick = Select-CompatibleJavaHome
    if ($pick) {
        $env:JAVA_HOME = $pick
        $env:PATH = (Join-Path $pick "bin") + ";" + $env:PATH
    } else {
        Write-Warning "Không tìm thấy JDK 8/11/17. Cài đặt JDK 17 rồi chạy lại."
        Write-Host   "  Tải: https://adoptium.net/temurin/releases/?version=17"
    }
}

# ─────── Cấu hình Data Lake (MinIO / S3) ───────
if (-not $env:S3_ENDPOINT)    { $env:S3_ENDPOINT    = "http://localhost:9000" }
if (-not $env:S3_ACCESS_KEY)  { $env:S3_ACCESS_KEY  = "minioadmin" }
if (-not $env:S3_SECRET_KEY)  { $env:S3_SECRET_KEY  = "minioadmin" }
if (-not $env:DELTA_BASE_PATH){ $env:DELTA_BASE_PATH= "s3a://delta-lake/delta" }
if (-not $env:CHECKPOINT_PATH){ $env:CHECKPOINT_PATH= "s3a://checkpoints/cdc" }
if (-not $env:TRIGGER_INTERVAL){$env:TRIGGER_INTERVAL= "10 seconds" }
if (-not $env:KAFKA_BOOTSTRAP){ $env:KAFKA_BOOTSTRAP = "localhost:9092" }

# ─────── --add-opens cho Java module system ───────
$addOpens = @(
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
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
) -join " "

$packages = @(
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
    "io.delta:delta-core_2.12:2.4.0",
    "org.apache.kafka:kafka-clients:3.4.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
) -join ","

Write-Host "============================================"
Write-Host "  Khởi động CDC Consumer (Windows)"
Write-Host "============================================"
Write-Host "  JAVA_HOME : $env:JAVA_HOME"
Write-Host "  KAFKA     : $env:KAFKA_BOOTSTRAP"
Write-Host "  DELTA     : $env:DELTA_BASE_PATH"
Write-Host "  CHECKPOINT: $env:CHECKPOINT_PATH"
Write-Host "  S3_ENDPT  : $env:S3_ENDPOINT"
Write-Host "  TRIGGER   : $env:TRIGGER_INTERVAL"
Write-Host ""

# Path local thì tạo thư mục trước
if ($env:DELTA_BASE_PATH -notmatch '^(s3a|s3|hdfs)://') {
    New-Item -ItemType Directory -Force -Path $env:DELTA_BASE_PATH | Out-Null
}
if ($env:CHECKPOINT_PATH -notmatch '^(s3a|s3|hdfs)://') {
    foreach ($t in "orders","order_products","products") {
        New-Item -ItemType Directory -Force -Path (Join-Path $env:CHECKPOINT_PATH $t) | Out-Null
    }
}

spark-submit `
    --master "local[4]" `
    --driver-memory 2g `
    --executor-memory 2g `
    --conf spark.sql.shuffle.partitions=4 `
    --conf spark.default.parallelism=4 `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --conf "spark.hadoop.fs.s3a.endpoint=$env:S3_ENDPOINT" `
    --conf "spark.hadoop.fs.s3a.access.key=$env:S3_ACCESS_KEY" `
    --conf "spark.hadoop.fs.s3a.secret.key=$env:S3_SECRET_KEY" `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider `
    --conf spark.hadoop.fs.s3a.committer.name=directory `
    --conf "spark.driver.extraJavaOptions=$addOpens" `
    --conf "spark.executor.extraJavaOptions=$addOpens" `
    --packages $packages `
    spark/cdc_consumer.py `
    @args
