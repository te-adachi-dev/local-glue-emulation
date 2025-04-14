#!/bin/bash

# Trinoの設定フォルダを作成
mkdir -p conf/trino-conf/catalog

# 必要な設定ファイルを作成
cat > conf/trino-conf/node.properties << 'EOF'
node.environment=test
node.id=trino-coordinator
node.data-dir=/data
EOF

cat > conf/trino-conf/config.properties << 'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://localhost:8080
EOF

cat > conf/trino-conf/jvm.config << 'EOF'
-server
-Xmx1G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
EOF

cat > conf/trino-conf/catalog/hive.properties << 'EOF'
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.allow-drop-table=true
hive.allow-rename-table=true
EOF

# 必要なディレクトリを作成して権限を設定
mkdir -p trino-data hive-data
chmod -R 777 trino-data hive-data
