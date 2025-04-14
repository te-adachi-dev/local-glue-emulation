# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import argparse
from datetime import datetime
import subprocess

def run_local_crawler(input_path, hive_metastore_uri="thrift://localhost:9083"):
    """
    ローカルでのクローラー処理を実装し、Hiveメタストアに登録する
    """
    schema_info = {}
    database_name = "test_db_20250407"
    
    # 入力ディレクトリ内のファイルを走査
    for root, _, files in os.walk(input_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file)[1].lower()
            
            table_name = os.path.splitext(file)[0]
            absolute_path = os.path.abspath(file_path)
            
            # ファイル形式に応じてスキーマ情報を抽出
            if file_ext == '.csv':
                df = pd.read_csv(file_path, nrows=10)
                columns = []
                
                for col in df.columns:
                    dtype = df[col].dtype
                    if 'int' in str(dtype):
                        hive_type = 'INT'
                    elif 'float' in str(dtype):
                        hive_type = 'DOUBLE'
                    elif 'bool' in str(dtype):
                        hive_type = 'BOOLEAN'
                    elif 'datetime' in str(dtype):
                        hive_type = 'TIMESTAMP'
                    else:
                        hive_type = 'STRING'
                        
                    columns.append({
                        'name': col,
                        'type': hive_type
                    })
                
                schema_info[table_name] = {
                    'format': 'csv',
                    'location': absolute_path,
                    'columns': columns
                }
                
                # Hive DDL生成
                hive_ddl = generate_hive_ddl(database_name, table_name, columns, absolute_path, 'csv')
                
                # Hiveメタストアにテーブル登録
                execute_hive_ddl(hive_ddl, hive_metastore_uri)
                
            elif file_ext == '.json':
                # JSONファイルの処理
                with open(file_path, 'r') as f:
                    try:
                        # サンプルデータからスキーマを推定
                        sample_data = json.load(f)
                        if isinstance(sample_data, list) and len(sample_data) > 0:
                            sample_item = sample_data[0]
                        elif isinstance(sample_data, dict):
                            sample_item = sample_data
                        else:
                            continue
                            
                        columns = []
                        for key, value in sample_item.items():
                            if isinstance(value, int):
                                hive_type = 'INT'
                            elif isinstance(value, float):
                                hive_type = 'DOUBLE'
                            elif isinstance(value, bool):
                                hive_type = 'BOOLEAN'
                            else:
                                hive_type = 'STRING'
                                
                            columns.append({
                                'name': key,
                                'type': hive_type
                            })
                        
                        schema_info[table_name] = {
                            'format': 'json',
                            'location': absolute_path,
                            'columns': columns
                        }
                        
                        # Hive DDL生成
                        hive_ddl = generate_hive_ddl(database_name, table_name, columns, absolute_path, 'json')
                        
                        # Hiveメタストアにテーブル登録
                        execute_hive_ddl(hive_ddl, hive_metastore_uri)
                    except json.JSONDecodeError:
                        print(f"Invalid JSON file: {file_path}")
                
    return schema_info

def generate_hive_ddl(database, table, columns, location, format):
    """
    Hive DDLを生成する
    """
    # データベース作成
    create_db = f"CREATE DATABASE IF NOT EXISTS {database};"
    
    # カラム定義
    column_defs = []
    for col in columns:
        column_defs.append(f"`{col['name']}` {col['type']}")
    
    column_str = ",\n  ".join(column_defs)
    
    # テーブル作成
    if format == 'csv':
        create_table = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
          {column_str}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{location}'
        TBLPROPERTIES ('skip.header.line.count'='1');
        """
    elif format == 'json':
        create_table = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
          {column_str}
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION '{location}';
        """
    
    return create_db + "\n" + create_table

def execute_hive_ddl(ddl, metastore_uri):
    """
    Hive DDLを実行する
    """
    # 一時ファイルに保存
    with open('temp_ddl.hql', 'w') as f:
        f.write(ddl)
    
    # Beeline経由でHiveメタストアにDDLを実行
    cmd = [
        "docker", "exec", "hive_metastore_20250407",
        "beeline", "-u", f"jdbc:hive2://{metastore_uri}",
        "-f", "/opt/hive/temp_ddl.hql"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"Successfully executed DDL")
    except subprocess.CalledProcessError as e:
        print(f"Error executing DDL: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run local crawler to scan and register data to Hive Metastore')
    parser.add_argument('--input_path', type=str, required=True, help='Input directory path containing data files')
    
    args = parser.parse_args()
    
    run_local_crawler(args.input_path)