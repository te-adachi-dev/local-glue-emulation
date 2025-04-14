#!/bin/bash

# 引数がない場合は使用方法を表示
if [ $# -lt 1 ]; then
    echo "Usage: $0 <sql_query> [output_file]"
    echo "Example: $0 \"SELECT * FROM test_db_20250407.test_table LIMIT 10\" results.csv"
    exit 1
fi

QUERY="$1"
OUTPUT_FILE="$2"

# 出力ファイルの指定があるかどうかで処理を分岐
if [ -n "$OUTPUT_FILE" ]; then
    docker exec glue20250407 python /home/glue_user/workspace/tools/execute_sql.py \
        --query "$QUERY" \
        --output /home/glue_user/workspace/data/output/query_results/$OUTPUT_FILE
else
    docker exec glue20250407 python /home/glue_user/workspace/tools/execute_sql.py \
        --query "$QUERY"
fi
