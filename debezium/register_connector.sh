#!/bin/bash
# ============================================================
# register_connector.sh
# Đăng ký Debezium MySQL Connector qua REST API
# Chạy sau khi docker-compose up và Debezium đã healthy
# ============================================================

DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_NAME="instacart-mysql-connector"

echo "============================================"
echo "  Đăng ký Debezium MySQL Connector"
echo "============================================"

# Chờ Debezium Connect sẵn sàng
echo "[1/3] Chờ Debezium Connect khởi động..."
until curl -sf "$DEBEZIUM_URL/connectors" > /dev/null; do
    echo "  ... chưa sẵn sàng, thử lại sau 5 giây"
    sleep 5
done
echo "  => Debezium Connect đã sẵn sàng!"

# Xóa connector cũ nếu tồn tại
EXISTING=$(curl -s "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME" | grep -c '"name"')
if [ "$EXISTING" -gt 0 ]; then
    echo "[2/3] Xóa connector cũ..."
    curl -s -X DELETE "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME"
    sleep 2
    echo "  => Đã xóa connector cũ"
else
    echo "[2/3] Chưa có connector cũ, tiếp tục..."
fi

# Đăng ký connector mới
echo "[3/3] Đăng ký connector mới..."
RESPONSE=$(curl -s -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'"$CONNECTOR_NAME"'",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",

      "database.hostname": "mysql",
      "database.port": "3306",
      "database.user": "cdc_user",
      "database.password": "cdc_pass",
      "database.server.id": "12345",

      "topic.prefix": "instacart",
      "database.include.list": "instacart",
      "table.include.list": "instacart.orders,instacart.order_products,instacart.products",

      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
      "schema.history.internal.kafka.topic": "instacart.schema-history",

      "include.schema.changes": "true",
      "snapshot.mode": "initial",

      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.add.fields": "op,table,source.ts_ms",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.unwrap.drop.tombstones": "false",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }')

echo ""
echo "Response từ Debezium:"
echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

# Kiểm tra kết quả
if echo "$RESPONSE" | grep -q '"name"'; then
    echo ""
    echo "============================================"
    echo "  THÀNH CÔNG! Connector đã được đăng ký"
    echo "============================================"
    echo ""
    echo "Kafka topics sẽ được tạo tự động:"
    echo "  - instacart.instacart.orders"
    echo "  - instacart.instacart.order_products"
    echo "  - instacart.instacart.products"
    echo ""
    echo "Kiểm tra trạng thái:"
    echo "  curl http://localhost:8083/connectors/$CONNECTOR_NAME/status"
else
    echo ""
    echo "LỖI! Kiểm tra lại Docker Compose đang chạy."
    exit 1
fi
