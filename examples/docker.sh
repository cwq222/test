#!/bin/bash

# Arrow Flight SQL Test Script
# ��用 Docker 运行测试

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 读取配置
DORIS_PROPS="$PROJECT_DIR/src/main/resources/doris.properties"
DORIS_HOST=$(grep "^doris.host=" "$DORIS_PROPS" | cut -d'=' -f2)
DORIS_ARROW_PORT=$(grep "^doris.arrow.port=" "$DORIS_PROPS" | cut -d'=' -f2)
DORIS_USER=$(grep "^doris.username=" "$DORIS_PROPS" | cut -d'=' -f2)
DORIS_PASSWORD=$(grep "^doris.password=" "$DORIS_PROPS" | cut -d'=' -f2)

# 默认查询
QUERY="${1:-SELECT * FROM test_db.test_table LIMIT 100}"

echo "=========================================="
echo "Arrow Flight SQL Test (Docker)"
echo "=========================================="
echo "Doris Host: $DORIS_HOST:$DORIS_ARROW_PORT"
echo "Query: $QUERY"
echo "=========================================="
echo

# 检查 JAR 文件
JAR_FILE="$PROJECT_DIR/target/arrow-flight-sql-test-1.0.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    echo "Please run: mvn clean package"
    exit 1
fi

# 运行 Docker
docker run --rm \
  --network doris_custom_network \
  -v "$JAR_FILE:/opt/spark/jars/arrow-flight-sql-test.jar" \
  apache/spark:3.5.8 \
  /opt/spark/bin/spark-submit \
  --master local[*] \
  --driver-java-options "--add-opens=java.base/java.nio=ALL-UNNAMED" \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties" \
  --conf arrow.host="$DORIS_HOST" \
  --conf arrow.port="$DORIS_ARROW_PORT" \
  --conf arrow.user="$DORIS_USER" \
  --conf arrow.password="$DORIS_PASSWORD" \
  --conf arrow.query="$QUERY" \
  --class com.portofino.arrow.test.ArrowFlightSQLTest \
  /opt/spark/jars/arrow-flight-sql-test.jar

echo
echo "=========================================="
echo "Test completed"
echo "=========================================="
