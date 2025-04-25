#!/bin/bash

# Trinoの設定を準備するスクリプト

echo "Trinoの設定ファイルを準備しています..."

# カタログディレクトリが存在するか確認
if [ ! -d "./conf/trino-conf/catalog" ]; then
  mkdir -p ./conf/trino-conf/catalog
fi

# Hiveカタログ設定
cat > ./conf/trino-conf/catalog/hive.properties << EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://hive-metastore:9083
hive.non-managed-table-writes-enabled=true
hive.collect-column-statistics-on-write=true
EOF

# config.properties
cat > ./conf/trino-conf/config.properties << EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF

# jvm.config
cat > ./conf/trino-conf/jvm.config << EOF
-server
-Xmx4G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
EOF

# node.properties
cat > ./conf/trino-conf/node.properties << EOF
node.environment=local
node.id=trino-local
node.data-dir=/data
EOF

echo "Trinoの設定が完了しました。"
