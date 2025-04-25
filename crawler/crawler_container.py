#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import logging
import re
import hashlib
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('crawler')

class LocalCrawler:
    """
    ローカル環境でのAWS Glueクローラーをエミュレートするクラス。
    CSVファイルからスキーマを推論し、Hiveテーブルを作成するDDLを生成します。
    日本語カラム名を英語カラム名に変換する機能を含みます。
    """

    # 既知のカラム名マッピング
    COLUMN_MAPPING = {
        'apid': 'apid',
        'ap名称': 'ap_name', 
        '設置場所名称': 'location',
        '都道府県': 'prefecture',
        '市区町村': 'city',
        '住所': 'address',
        '補足情報': 'additional_info',
        '緯度': 'latitude',
        '経度': 'longitude',
        'ステータス': 'status',
        '利用開始日時': 'start_time',
        '利用終了日時': 'end_time',
        'カテゴリ': 'category',
        '保有主体属性': 'owner_attr',
        '施設属性': 'facility_attr',
        '局属性': 'station_attr',
        '事業者': 'provider',
        'yearmonth': 'yearmonth',
        '利用者属性': 'user_attr',
        '端末属性': 'device_attr'
    }

    def __init__(self, input_dir: str = "./data/input/csv", 
                database_name: str = "test_db_20250407", 
                output_ddl: str = "/home/glue_user/temp_ddl.hql"):
        """
        クローラーの初期化
        
        Args:
            input_dir: 入力CSVファイルが格納されているディレクトリ
            database_name: 作成するデータベース名
            output_ddl: 出力DDLファイルのパス
        """
        self.input_dir = input_dir
        self.database_name = database_name
        self.output_ddl = output_ddl
        self.timestamp = datetime.now().strftime("%Y%m%d")
        self.unknown_columns = {}  # 未知カラム名のマッピングを保存
        
        # マッピング情報を保存するディレクトリ
        self.mapping_dir = os.path.join(os.path.dirname(self.output_ddl), "column_mappings")
        os.makedirs(self.mapping_dir, exist_ok=True)

    def generate_english_column_name(self, jp_name: str) -> str:
        """
        日本語カラム名から英語カラム名を生成します
        
        Args:
            jp_name: 日本語カラム名
            
        Returns:
            英語カラム名
        """
        # 既知のマッピングを優先
        if jp_name in self.COLUMN_MAPPING:
            return self.COLUMN_MAPPING[jp_name]
        
        # 既に英語/ASCII文字の場合はそのまま使用（小文字化して空白をアンダースコアに）
        if all(ord(c) < 128 for c in jp_name):
            return jp_name.lower().replace(" ", "_").replace("-", "_")
        
        # 未知の日本語カラム名の場合、一意の識別子を生成
        hash_value = hashlib.md5(jp_name.encode('utf-8')).hexdigest()[:4]
        safe_name = f"column_{hash_value}"
        
        # マッピングを記録
        self.unknown_columns[safe_name] = jp_name
        logger.info(f"未知のカラム名を変換: {jp_name} → {safe_name}")
        
        return safe_name

    def infer_types(self, row: List[str]) -> List[str]:
        """
        データ行から各カラムのデータ型を推論します
        
        Args:
            row: CSVの1行分のデータ
            
        Returns:
            推論された型のリスト
        """
        types = []
        for val in row:
            if val == "":
                # 空値の場合は文字列として扱う
                types.append("STRING")
            elif re.match(r'^-?\d+$', val):
                # 整数値
                types.append("INT")
            elif re.match(r'^-?\d+\.\d+$', val):
                # 浮動小数点数
                types.append("DOUBLE")
            elif re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}', val):
                # 日付形式
                types.append("STRING")  # 日付も文字列として扱う
            else:
                # その他は文字列
                types.append("STRING")
        return types

    def find_csv_files(self) -> List[str]:
        """
        指定ディレクトリからCSVファイルを検索します
        
        Returns:
            CSVファイルのパスのリスト
        """
        csv_files = []
        for file in os.listdir(self.input_dir):
            if file.endswith('.csv'):
                csv_files.append(os.path.join(self.input_dir, file))
        return csv_files

    def read_csv_header_and_sample(self, file_path: str) -> Tuple[List[str], List[str]]:
        """
        CSVファイルからヘッダーとサンプルデータを読み取ります
        
        Args:
            file_path: CSVファイルのパス
            
        Returns:
            ヘッダーとサンプルデータのタプル
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            headers = next(csv_reader)
            sample_row = next(csv_reader, None)
            return headers, sample_row

    def generate_table_ddl(self, table_name: str, headers: List[str], 
                         inferred_types: List[str]) -> List[str]:
        """
        テーブル作成用のDDLを生成します
        
        Args:
            table_name: テーブル名
            headers: カラム名のリスト
            inferred_types: 推論されたデータ型のリスト
            
        Returns:
            DDL文のリスト
        """
        ddl = []
        ddl.append(f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.{table_name} (")
        
        columns = []
        column_mappings = {}  # このテーブルのカラム名マッピングを保存
        
        for i, header in enumerate(headers):
            # 日本語→英語に変換
            column_name = self.generate_english_column_name(header)
            column = f"  {column_name} {inferred_types[i]}"
            columns.append(column)
            
            # マッピング情報を保存
            column_mappings[column_name] = header
        
        ddl.append(",\n".join(columns))
        ddl.append(")")
        ddl.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")
        ddl.append("WITH SERDEPROPERTIES (")
        ddl.append('  "separatorChar" = ",",')
        ddl.append('  "quoteChar" = "\\"",')
        ddl.append('  "escapeChar" = "\\\\"')
        ddl.append(")")
        ddl.append("STORED AS TEXTFILE")
        ddl.append(f"LOCATION '/opt/hive/warehouse/{table_name}'")
        ddl.append("TBLPROPERTIES ('skip.header.line.count'='1');")
        
        # カラム名マッピングを保存
        mapping_file = os.path.join(self.mapping_dir, f"{table_name}_mapping.csv")
        with open(mapping_file, 'w', encoding='utf-8') as f:
            f.write("英語カラム名,元のカラム名\n")
            for eng, jp in column_mappings.items():
                f.write(f"{eng},{jp}\n")
                
        logger.info(f"カラム名マッピングを保存: {mapping_file}")
        
        return ddl

    def run(self):
        """
        クローラーを実行し、DDLファイルを生成します
        """
        logger.info("Starting crawler in container...")
        
        csv_files = self.find_csv_files()
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # DDL文を格納するリスト
        ddl_statements = []
        
        # データベース作成DDL
        ddl_statements.append(f"CREATE DATABASE IF NOT EXISTS {self.database_name};")
        ddl_statements.append("")  # 空行を追加
        
        for csv_file in csv_files:
            # ファイル名からテーブル名を取得
            file_name = os.path.basename(csv_file)
            table_name = os.path.splitext(file_name)[0]
            
            # ヘッダーとサンプルデータを読み取り
            headers, sample_row = self.read_csv_header_and_sample(csv_file)
            
            logger.info(f"Table: {table_name}, Headers: {headers}")
            logger.info(f"Sample data row: {sample_row}")
            
            # データ型を推論
            inferred_types = self.infer_types(sample_row)
            logger.info(f"Inferred schema for {table_name}: {list(zip(headers, inferred_types))}")
            
            # テーブル作成DDLを生成
            table_ddl = self.generate_table_ddl(table_name, headers, inferred_types)
            
            # 全体のDDLに追加
            ddl_statements.extend(table_ddl)
            ddl_statements.append("")  # 空行を追加
            
            logger.info(f"Added table: {table_name}")
        
        # DDLファイルに書き出し
        with open(self.output_ddl, 'w', encoding='utf-8') as f:
            f.write("\n".join(ddl_statements))
        
        logger.info(f"Crawler completed. DDL file generated at: {self.output_ddl}")
        return True

if __name__ == "__main__":
    # クローラーを実行
    crawler = LocalCrawler()
    crawler.run()