#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import logging
import shutil
import pandas as pd
from local_spark_session import get_spark_session

# ロギングの設定
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('setup_local_env')

def setup_local_environment():
    """
    ローカル環境のセットアップを行う
    """
    logger.info("=== 環境セットアップスクリプト ===")
    
    # プロジェクトのルートディレクトリ
    root_dir = os.getcwd()
    
    # 1. テストデータを準備
    logger.info("1. テストデータを準備中...")
    
    # ディレクトリ作成
    for dir_path in [
        os.path.join(root_dir, "data", "input", "csv"),
        os.path.join(root_dir, "data", "warehouse"),
        os.path.join(root_dir, "data", "output", "processed_data"),
        os.path.join(root_dir, "data", "output", "query_results"),
        os.path.join(root_dir, "spark-warehouse")
    ]:
        os.makedirs(dir_path, exist_ok=True)
    
    # テストデータ作成
    test_data_path = os.path.join(root_dir, "data", "input", "csv", "test_table.csv")
    
    test_data = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["テスト太郎", "テスト花子", "テスト次郎", "テスト三郎"],
        "age": [30, 25, 40, 35],
        "department": ["開発部", "営業部", "総務部", "人事部"]
    })
    
    test_data.to_csv(test_data_path, index=False, encoding='utf-8')
    logger.info(f"テストデータを作成: {test_data_path}")
    
    # 社員データも作成
    employee_data_path = os.path.join(root_dir, "data", "input", "csv", "employee_data.csv")
    
    employee_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "employee_name": ["山田太郎", "鈴木花子", "佐藤次郎", "田中三郎", "伊藤四郎"],
        "department": ["開発部", "営業部", "総務部", "人事部", "開発部"],
        "salary": [400000, 350000, 450000, 380000, 420000],
        "hire_date": ["2020-04-01", "2021-07-15", "2019-01-10", "2022-03-20", "2020-06-05"]
    })
    
    employee_data.to_csv(employee_data_path, index=False, encoding='utf-8')
    logger.info(f"社員データを作成: {employee_data_path}")
    
    # WiFiアクセスログを作成
    wifi_data_path = os.path.join(root_dir, "data", "input", "csv", "wifi_access_logs.csv")
    
    wifi_data = pd.DataFrame({
        "mac_address": ["00:1A:2B:3C:4D:5E", "00:2B:3C:4D:5E:6F", "00:3C:4D:5E:6F:7A"],
        "connection_start": ["2025-04-15 10:00:00", "2025-04-15 10:05:00", "2025-04-15 10:10:00"],
        "connection_end": ["2025-04-15 10:30:00", "2025-04-15 10:45:00", "2025-04-15 11:00:00"],
        "access_point_id": ["AP_001", "AP_002", "AP_001"],
        "signal_strength": ["-65 dBm", "-70 dBm", "-60 dBm"]
    })
    
    wifi_data.to_csv(wifi_data_path, index=False, encoding='utf-8')
    logger.info(f"WiFiアクセスログを作成: {wifi_data_path}")
    
    # 2. データウェアハウスディレクトリの準備
    logger.info("2. データウェアハウスディレクトリの準備...")
    
    # テストデータをウェアハウスディレクトリにコピー
    for src_file, dest_dir in [
        (test_data_path, os.path.join(root_dir, "data", "warehouse", "test_table")),
        (employee_data_path, os.path.join(root_dir, "data", "warehouse", "employee_data")),
        (wifi_data_path, os.path.join(root_dir, "data", "warehouse", "wifi_access_logs"))
    ]:
        os.makedirs(dest_dir, exist_ok=True)
        shutil.copy(src_file, dest_dir)
    
    # 3. Sparkセッションの初期化と確認
    logger.info("3. Sparkセッションの初期化と確認...")
    
    try:
        # Sparkセッションの初期化
        spark, sc, _ = get_spark_session("SetupEnvironment")
        
        # 4. テーブルの作成 - Sparkの一時テーブルとして
        logger.info("4. テーブルの作成...")
        
        # CSVファイルから直接読み込み、テーブルとして登録
        for file_path, table_name in [
            (test_data_path, "test_table"),
            (employee_data_path, "employee_data"),
            (wifi_data_path, "wifi_access_logs")
        ]:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
            df.createOrReplaceTempView(table_name)
            logger.info(f"テーブル {table_name} を作成しました（一時テーブル）")
        
        # テーブルデータ確認
        logger.info("テストテーブルのデータ:")
        spark.sql("SELECT * FROM test_table").show(5)
        
        # 5. DDLファイルの生成
        logger.info("5. DDLファイルの生成...")
        
        # temp_ddl.hqlの作成
        ddl_path = os.path.join(root_dir, "temp_ddl.hql")
        database_name = "test_db_20250415"
        
        with open(ddl_path, 'w') as f:
            f.write(f"-- このDDLは参照用です。実際のテーブルはメモリ上に作成されます。\n\n")
            f.write(f"CREATE DATABASE IF NOT EXISTS {database_name};\n\n")
            
            # テストテーブルのDDL
            f.write(f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.test_table (
  `id` INT,
  `name` STRING,
  `age` INT,
  `department` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{os.path.join(root_dir, "data", "warehouse", "test_table").replace("\\", "/")}'
TBLPROPERTIES ('skip.header.line.count'='1');\n\n""")
            
            # 社員データテーブルのDDL
            f.write(f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.employee_data (
  `id` INT,
  `employee_name` STRING,
  `department` STRING,
  `salary` INT,
  `hire_date` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{os.path.join(root_dir, "data", "warehouse", "employee_data").replace("\\", "/")}'
TBLPROPERTIES ('skip.header.line.count'='1');\n\n""")
            
            # WiFiアクセスログテーブルのDDL
            f.write(f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.wifi_access_logs (
  `mac_address` STRING,
  `connection_start` STRING,
  `connection_end` STRING,
  `access_point_id` STRING,
  `signal_strength` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{os.path.join(root_dir, "data", "warehouse", "wifi_access_logs").replace("\\", "/")}'
TBLPROPERTIES ('skip.header.line.count'='1');\n\n""")
        
        logger.info(f"DDLファイルを作成: {ddl_path}")
        logger.info("環境セットアップが完了しました！")
        
    except Exception as e:
        logger.error(f"Spark初期化中にエラーが発生しました: {str(e)}")
        logger.info("Spark初期化をスキップしますが、データファイルは準備されました。")
    
    return True

if __name__ == "__main__":
    setup_local_environment()