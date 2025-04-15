#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import argparse
import logging
import pandas as pd
from local_spark_session import get_spark_session

# ロギングの設定
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('local_crawler')

def run_local_crawler(input_path, database_name="test_db_20250415"):
    """
    ローカル環境でのクローラー処理を実装
    """
    logger.info("Starting local crawler...")
    
    # Sparkセッションの初期化
    spark, sc, glue_context = get_spark_session("LocalCrawler")
    
    # 戻り値用にテーブル情報を格納
    registered_tables = []
    
    # 入力ディレクトリ内のCSVファイルを検索
    for root, _, files in os.walk(input_path):
        for file in files:
            if file.endswith('.csv'):
                table_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)
                
                logger.info(f"Processing CSV file: {file_path}")
                
                try:
                    # CSVファイルの読み込み
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                    
                    # スキーマ情報のログ出力
                    logger.info(f"Table: {table_name}, Columns: {', '.join(df.columns)}")
                    
                    # テーブルの登録 (一時テーブル)
                    df.createOrReplaceTempView(table_name)
                    
                    # テーブル情報を記録
                    registered_tables.append({
                        "table": table_name,
                        "columns": df.columns,
                        "file_path": file_path
                    })
                    
                    logger.info(f"Successfully registered table: {table_name}")
                    
                    # サンプルデータの表示
                    logger.info(f"Sample data for {table_name}:")
                    df.show(5, truncate=False)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
            
            elif file.endswith('.json'):
                # JSONファイルのサポートも同様に実装
                table_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)
                
                logger.info(f"Processing JSON file: {file_path}")
                
                try:
                    # JSONファイルの読み込み
                    df = spark.read.option("multiLine", "true").json(file_path)
                    
                    # テーブルの登録 (一時テーブル)
                    df.createOrReplaceTempView(table_name)
                    
                    # テーブル情報を記録
                    registered_tables.append({
                        "table": table_name,
                        "columns": df.columns,
                        "file_path": file_path
                    })
                    
                    logger.info(f"Successfully registered table: {table_name}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
    
    # 登録テーブルの一覧を表示
    logger.info(f"Total registered tables: {len(registered_tables)}")
    for table_info in registered_tables:
        logger.info(f"- {table_info['table']}")
    
    # クローラー結果をDDLファイルとして保存
    generate_reference_ddl(registered_tables, database_name)
    
    return registered_tables

def generate_reference_ddl(table_infos, database_name):
    """
    登録されたテーブル情報から参照用のDDLを生成する
    """
    # DDLファイルパス
    ddl_path = os.path.join(os.getcwd(), "crawler_output.hql")
    
    with open(ddl_path, 'w') as f:
        # データベース作成
        f.write(f"-- このDDLは参照用です。実際のテーブルはメモリ上に作成されています。\n")
        f.write(f"CREATE DATABASE IF NOT EXISTS {database_name};\n\n")
        
        # 各テーブルのDDL
        for table_info in table_infos:
            table_name = table_info["table"]
            columns = table_info["columns"]
            file_path = table_info["file_path"]
            
            # シンプルな型推測 (実際のデータを見て推測すべき)
            column_defs = []
            for col in columns:
                column_defs.append(f"`{col}` STRING")
            
            column_str = ",\n  ".join(column_defs)
            warehouse_dir = os.path.join(os.getcwd(), "data", "warehouse", table_name)
            
            # テーブル作成DDL
            create_table = f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
  {column_str}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{warehouse_dir.replace("\\", "/")}'
TBLPROPERTIES ('skip.header.line.count'='1');\n\n"""
            
            f.write(create_table)
    
    logger.info(f"Generated reference DDL file: {ddl_path}")
    return ddl_path

def main():
    parser = argparse.ArgumentParser(description='Run local crawler to scan data files')
    parser.add_argument('--input_path', type=str, default='./data/input',
                        help='Input directory path containing data files')
    parser.add_argument('--database', type=str, default='test_db_20250415',
                        help='Reference database name')
    
    args = parser.parse_args()
    
    # クローラーの実行
    run_local_crawler(args.input_path, args.database)

if __name__ == "__main__":
    main()