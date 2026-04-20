# DoAn21

### Run docker
```
docker-compose up -d
```

### Stop docker
```
docker-compose down
```


### Check if Debezium API is responding
```
curl http://localhost:8083/connectors
```

### Get Debezium status
```
curl http://localhost:8083/health/live
```


### Create a MySQL connector in Debezium
```
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql",
      "database.port": 3306,
      "database.user": "root",
      "database.password": "root",
      "database.server.id": 1,
      "database.server.name": "mysql",
      "topic.prefix": "mysql",
      "table.include.list": "instacart.*",
      "schema.history.internal.kafka.topic": "_schema-history.mysql",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092"
    }
  }'
```