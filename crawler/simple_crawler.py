# -*- coding: utf-8 -*-
import os
import argparse
import subprocess

def run_simple_crawler(input_path):
    """
    シンプルなクローラー。CSVファイルを見つけて、定型的なDDLを生成して実行する
    """
    # テスト用データベース名
    database_name = "test_db_20250407"
    
    # データベース作成DDL
    create_db_ddl = "CREATE DATABASE IF NOT EXISTS {};".format(database_name)
    
    # テーブルスキーマ (手動で定義)
    table_schemas = {
        "test_table": [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "STRING"},
            {"name": "value", "type": "DOUBLE"}
        ]
    }
    
    # 一時DDLファイル
    with open('temp_ddl.hql', 'w') as f:
        f.write(create_db_ddl + "\n")
        
        # 入力ディレクトリ内のCSVファイルを検索
        for root, _, files in os.walk(input_path):
            for file in files:
                if file.endswith('.csv'):
                    table_name = os.path.splitext(file)[0]
                    file_path = os.path.join(root, file)
                    absolute_path = os.path.abspath(file_path)
                    
                    # テーブル定義があればそれを使用、なければデフォルト
                    columns = table_schemas.get(table_name, [
                        {"name": "col1", "type": "STRING"},
                        {"name": "col2", "type": "STRING"},
                        {"name": "col3", "type": "STRING"}
                    ])
                    
                    column_defs = []
                    for col in columns:
                        column_defs.append("`{}` {}".format(col['name'], col['type']))
                    
                    column_str = ",\n  ".join(column_defs)
                    
                    create_table = """
                    CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (
                      {}
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '{}'
                    TBLPROPERTIES ('skip.header.line.count'='1');
                    """.format(database_name, table_name, column_str, absolute_path)
                    
                    f.write(create_table + "\n")
                    print("Added DDL for table: {}".format(table_name))
    
    # Beeline経由でHiveメタストアにDDLを実行
    metastore_uri = "thrift://localhost:9083"
    cmd = [
        "docker", "exec", "hive_metastore_20250407",
        "beeline", "-u", "jdbc:hive2://{}".format(metastore_uri),
        "-f", "/opt/hive/temp_ddl.hql"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print("Successfully executed DDL")
    except subprocess.CalledProcessError as e:
        print("Error executing DDL: {}".format(e))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple crawler to register CSV files to Hive Metastore')
    parser.add_argument('--input_path', type=str, required=True, help='Input directory path containing data files')
    
    args = parser.parse_args()
    
    run_simple_crawler(args.input_path)
