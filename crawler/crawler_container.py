#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import csv
import json
import sys
import logging

# ロギングの設定
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('crawler')

def run_crawler():
    """
    シンプルなクローラー。ファイルを見つけて、CREATEステートメントを生成する
    """
    logger.info("Starting crawler in container...")
    
    # 入力ディレクトリを指定
    input_dir = "/home/glue_user/workspace/data/input"
    warehouse_dir = "/home/glue_user/workspace/data/warehouse"
    database_name = "test_db_20250407"
    
    # ユーザーのホームディレクトリにDDLファイルを作成 (書き込み権限あり)
    ddl_path = "/home/glue_user/temp_ddl.hql"
    
    try:
        with open(ddl_path, 'w') as ddl_file:
            # データベース作成
            ddl_file.write(f"CREATE DATABASE IF NOT EXISTS {database_name};\n\n")
            
            # CSVファイルの検索
            csv_files = []
            csv_dir = os.path.join(input_dir, "csv")
            
            if not os.path.exists(csv_dir):
                logger.warning(f"CSV directory not found: {csv_dir}")
            
            for root, _, files in os.walk(csv_dir):
                for filename in files:
                    if filename.endswith('.csv'):
                        file_path = os.path.join(root, filename)
                        csv_files.append((filename, file_path))
            
            logger.info(f"Found {len(csv_files)} CSV files")
            
            # 各CSVファイルを処理
            for filename, file_path in csv_files:
                table_name = os.path.splitext(filename)[0]
                warehouse_table_dir = os.path.join(warehouse_dir, table_name)
                
                # ディレクトリが存在しない場合は作成
                os.makedirs(warehouse_table_dir, exist_ok=True)
                
                # CSVファイルをウェアハウスにコピー
                with open(file_path, 'r') as src, open(os.path.join(warehouse_table_dir, filename), 'w') as dst:
                    dst.write(src.read())
                
                # スキーマの推測
                columns = []
                try:
                    with open(file_path, 'r') as f:
                        reader = csv.reader(f)
                        headers = next(reader)  # ヘッダー行を取得
                        
                        # デバッグ情報
                        logger.info(f"Table: {table_name}, Headers: {headers}")
                        
                        # 1行目のデータを取得してデータ型を推測
                        data_row = next(reader, None)
                        if data_row:
                            logger.info(f"Sample data row: {data_row}")
                            for i, val in enumerate(data_row):
                                col_name = headers[i] if i < len(headers) else f"col{i+1}"
                                # データ型の推測
                                try:
                                    int(val)
                                    col_type = "INT"
                                except ValueError:
                                    try:
                                        float(val)
                                        col_type = "DOUBLE"
                                    except ValueError:
                                        if val.lower() in ('true', 'false'):
                                            col_type = "BOOLEAN"
                                        else:
                                            col_type = "STRING"
                                columns.append((col_name, col_type))
                        else:
                            # データがなければヘッダーだけでSTRINGとして設定
                            columns = [(h, "STRING") for h in headers]
                except Exception as e:
                    logger.error(f"Error analyzing CSV {filename}: {e}")
                    # 何か問題があれば、デフォルトのスキーマを使用
                    columns = [("col1", "STRING"), ("col2", "STRING"), ("col3", "STRING")]
                
                # 推測されたスキーマの確認
                logger.info(f"Inferred schema for {table_name}: {columns}")
                
                # DDL文の作成
                column_defs = [f"`{name}` {type}" for name, type in columns]
                column_str = ",\n  ".join(column_defs)
                create_table = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
  {column_str}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{warehouse_table_dir}'
TBLPROPERTIES ('skip.header.line.count'='1');"""
                
                ddl_file.write(create_table + "\n\n")
                logger.info(f"Added table: {table_name}")
        
        logger.info(f"Crawler completed. DDL file generated at: {ddl_path}")
        return True
    except Exception as e:
        logger.error(f"Error in crawler: {e}")
        return False

if __name__ == "__main__":
    success = run_crawler()
    sys.exit(0 if success else 1)