#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import logging
import pandas as pd
from local_spark_session import get_spark_session

# ロギングの設定
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('sql_executor')

def execute_sql(query, output_file=None):
    """
    ローカルSparkでSQLクエリを実行し、結果を表示またはファイルに保存する
    """
    try:
        # Sparkセッションの初期化
        spark, _, _ = get_spark_session("SQLExecutor")
        
        logger.info(f"Executing query: {query}")
        
        # 入力ディレクトリのCSVファイルを読み込む（テーブルが見つからない場合）
        input_dir = os.path.join(os.getcwd(), "data", "input", "csv")
        for file in os.listdir(input_dir):
            if file.endswith('.csv'):
                table_name = os.path.splitext(file)[0]
                file_path = os.path.join(input_dir, file)
                try:
                    # テーブルがあるか確認
                    spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
                except:
                    # テーブルが見つからない場合は作成
                    logger.info(f"Loading table {table_name} from {file_path}")
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                    df.createOrReplaceTempView(table_name)
        
        # クエリの実行
        result = spark.sql(query)
        
        # 結果の表示
        row_count = result.count()
        logger.info(f"Query executed successfully. Result has {row_count} rows.")
        
        if row_count > 0:
            result.show(20, truncate=False)
        else:
            logger.info("No data returned by the query.")
        
        # 結果をファイルに保存
        if output_file:
            # 出力ディレクトリの作成
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # CSVとして保存
            result_pd = result.toPandas()
            result_pd.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Results saved to {output_file}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Execute SQL query against local Spark')
    parser.add_argument('--query', type=str, required=True,
                        help='SQL query to execute')
    parser.add_argument('--output', type=str,
                        help='Output file path (CSV format)')
    
    args = parser.parse_args()
    
    # SQLクエリの実行
    execute_sql(args.query, args.output)

if __name__ == "__main__":
    main()