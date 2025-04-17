#!/bin/bash

# 引数がない場合は使用方法を表示
if [ $# -lt 1 ]; then
    echo "Usage: $0 <sql_query> [output_file]"
    echo "Example: $0 \"SELECT * FROM test_db_20250407.test_table LIMIT 10\" results.csv"
    exit 1
fi

QUERY="$1"
OUTPUT_FILE="$2"

# 出力ディレクトリの確認
mkdir -p ./data/output/query_results

# SQL実行（Hiveコンテナを使用）
echo "Executing SQL query using Hive CLI..."
if [ -n "$OUTPUT_FILE" ]; then
    # 結果をファイルに保存
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "$QUERY" > ./data/output/query_results/$OUTPUT_FILE
    echo "Results saved to ./data/output/query_results/$OUTPUT_FILE"
else
    # 結果を表示
    docker exec hive_metastore_20250407 /opt/hive/bin/hive -e "$QUERY"
fi