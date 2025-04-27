#!/usr/bin/env python
# -*- coding: utf-8 -*-

#####ここからモック用変更####
import sys
import os
import logging
import pytz
from datetime import datetime
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, udf, year, month, dayofmonth, broadcast, to_timestamp
from pyspark.sql.types import TimestampType, StringType

# モック環境用の設定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Dockerコンテナ内での実行ディレクトリを考慮
if os.path.exists('/home/glue_user/workspace'):
    # コンテナ内での実行
    DATA_DIR = '/home/glue_user/data'
    INPUT_DIR = '/home/glue_user/data/input/csv'
    OUTPUT_DIR = '/home/glue_user/data/output/connection_output'
    COLUMN_MAPPINGS_DIR = '/home/glue_user/crawler/column_mappings'
else:
    # ローカルでの実行
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), "data")
    INPUT_DIR = os.path.join(DATA_DIR, "input", "csv")
    OUTPUT_DIR = os.path.join(DATA_DIR, "output", "connection_output")
    COLUMN_MAPPINGS_DIR = os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), "crawler", "column_mappings")

# 出力ディレクトリの作成
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 引数処理のモック
class MockArgs:
    def __init__(self, yyyymm=None, job_name="connection_job", job_run_id="mock_run_123"):
        self.YYYYMM = yyyymm if yyyymm else "202501"
        self.JOB_NAME = job_name
        self.JOB_RUN_ID = job_run_id

# SparkSessionのモック
def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# モック用のgetResolvedOptions関数
def getResolvedOptions(argv, args):
    # コマンドライン引数からパラメータを取得するモック
    yyyymm = None
    for i, arg in enumerate(argv):
        if arg == '--YYYYMM' and i+1 < len(argv):
            yyyymm = argv[i+1]
    return MockArgs(yyyymm=yyyymm)
####モック用変更ここまで###

#####本物Ｇｌｕｅと共通コード####
# 既存のディレクトリを削除
def delete_existing_directory(dir_path):
    logger.info(" 既存のフォルダ，ファイル削除を開始 ")
    
    if os.path.exists(dir_path):
        for item in os.listdir(dir_path):
            item_path = os.path.join(dir_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                for sub_item in os.listdir(item_path):
                    sub_item_path = os.path.join(item_path, sub_item)
                    if os.path.isfile(sub_item_path):
                        os.remove(sub_item_path)

# ファイルの読み込み
def load_csv(spark: SparkSession, file_path: str) -> pd.DataFrame:
    print(f"CSVファイルを読み込み中: {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

# 属性置換処理
def replace_attributes(df, df_user, df_device):
    logger.info(" 属性置換処理を開始 ")

    # 利用者属性の置換
    df = df.join(df_user, df["利用者属性"] == df_user["各社利用者属性"], "left") \
           .withColumn("利用者属性", when(col("共通ログ利用者属性").isNotNull(), col("共通ログ利用者属性")).otherwise("その他")) \
           .drop("各社利用者属性", "日本語表記", "共通ログ利用者属性")

    # 端末属性の置換
    df = df.join(df_device, df["端末属性"] == df_device["各社端末属性"], "left") \
           .withColumn("端末属性", when(col("共通ログ端末属性").isNotNull(), col("共通ログ端末属性")).otherwise("その他")) \
           .drop("各社端末属性", "共通ログ端末属性")

    # MACアドレス判定・管理ID設定
    df = df.withColumn(
        "管理ID",
        when(
            (col("APID").rlike(r"([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}")),  # MACアドレス形式の正規表現
            regexp_replace(col("APID"), r"[-:]", "")  # :や-を削除
        ).otherwise(col("APID"))
    )
    return df

# 利用開始日時をISO8601形式に変換し、年月と日を抽出
def process_dates(df):
    logger.info(" 日付処理を開始 ")

    # to_timestampで日付変換
    df = df.withColumn("利用開始日時", to_timestamp(col("利用開始日時"), "yyyy-MM-dd HH:mm:ss"))

    # 年月 (yyyymm形式) と 日 (dd) のカラムを抽出、NULLの場合はNoneを設定
    df = df.withColumn("年月", when(col("利用開始日時").isNotNull(), (year(col("利用開始日時")) * 100 + month(col("利用開始日時")))).otherwise(None))
    df = df.withColumn("日", when(col("利用開始日時").isNotNull(), dayofmonth(col("利用開始日時"))).otherwise(None))

    # 利用開始日時をstringとして扱う
    df = df.withColumn("利用開始日時", col("利用開始日時").cast(StringType()))

    return df

def main():
    # 環境情報のデバッグ出力
    print(f"現在の作業ディレクトリ: {os.getcwd()}")
    print(f"入力ディレクトリ: {INPUT_DIR}")
    print(f"出力ディレクトリ: {OUTPUT_DIR}")

    # 入力ファイルの確認
    print("入力ファイルの確認中...")
    if not os.path.exists(INPUT_DIR):
        logger.error(f"入力ディレクトリが存在しません: {INPUT_DIR}")
        sys.exit(1)
        
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            print(f"見つかったファイル: {file}")

    # Sparkセッションの作成
    spark = create_spark_session("ConnectionJob")

    # マスターデータの読み込み
    df_device = load_csv(spark, os.path.join(INPUT_DIR, "master_device_attribute.csv"))
    df_device = broadcast(df_device)
    df_user = load_csv(spark, os.path.join(INPUT_DIR, "master_user_attribute.csv"))
    df_user = broadcast(df_user)

    # input_connectionの読み込み（CSVファイル）
    input_file = os.path.join(INPUT_DIR, "input_connection.csv")
    print(f"入力ファイルパス: {input_file}")
    if not os.path.exists(input_file):
        logger.error(f"ファイルが存在しません: {input_file}")
        sys.exit(1)

    df = spark.read.csv(input_file, header=True, inferSchema=True)
    print(f"読み込んだカラム: {df.columns}")

    # 必須カラムチェック（フォーマットチェック）
    input_column_list = ["APID", "利用開始日時", "利用者属性", "端末属性", "事業者", "yearmonth"]
    existing_columns = df.columns
    expected_column_count = len(input_column_list)
    actual_column_count = len(existing_columns)

    # 必須カラム外のカラムが存在するリスト
    missing_columns = [colname for colname in input_column_list if colname not in existing_columns]

    if missing_columns:
        logger.error(f"ERROR: 必須カラムが不足しています -> {missing_columns}")
        sys.exit(1)

    if actual_column_count != expected_column_count:
        logger.error(f"ERROR: カラム数が一致しません (想定: {expected_column_count}, 実際: {actual_column_count})")
        sys.exit(1)

    # データフレームのレコードが0件かどうかのチェックを実施
    if df.count() == 0:
        # エラーログの出力
        error_message = "データ件数は0件です。データ内容を確認してください。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # 'APID'がない時にドロップ
    df_null_apid = df.filter(col("APID").isNull())
    
    # nullの行が存在するかどうかを確認
    if df_null_apid.count() > 0:
        error_message = "このレコードの形式が不正です。"
        log = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:WARN, Message:\"{error_message}\""
        data = df_null_apid.toPandas().to_dict(orient='records')
        log_output = log + ", data:" + ",".join([json.dumps(d, ensure_ascii=False) for d in data]) + "}"
        logger.warning(log_output)

    df = df.na.drop(subset=["APID"])

    # データフレームのレコードが0件かどうかのチェックを実施（APID NULLドロップ後）
    if df.count() == 0:
        # エラーログの出力
        error_message = "データ件数は0件です。データ内容を確認してください。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # 処理対象月のみに絞り込み
    print(f"処理対象年月: {args.YYYYMM}")
    df = df.filter(col("yearmonth") == args.YYYYMM)
    print(f"絞り込み後のレコード数: {df.count()}")

    # 属性置換
    df = replace_attributes(df, df_user, df_device)

    # 日付処理
    df = process_dates(df)

    # 必要なカラムを設定
    df = df.select("管理ID", "APID", "利用開始日時", "利用者属性", "端末属性", "年月", "日", "事業者")

    # 既存のディレクトリを削除
    delete_existing_directory(OUTPUT_DIR)

    # 出力前にデータフレーム内にデータが存在することをチェック
    record_count = df.count()
    print(f"出力レコード数: {record_count}")
    if record_count == 0:
        error_message = "出力ファイルにヘッダー以外の情報が含まれていません。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # パーティション付きで保存
    yearmonth_str = args.YYYYMM
    output_dir = os.path.join(OUTPUT_DIR, f"yearmonth={yearmonth_str}")
    os.makedirs(output_dir, exist_ok=True)
    
    # データフレームを保存
    output_path = os.path.join(output_dir, "connection_data.csv")
    df.toPandas().to_csv(output_path, index=False, encoding='utf-8')
    print(f"ファイルを保存しました: {output_path}")
    
    logger.info("INFO: ファイル出力が完了しました")
####本物Ｇｌｕｅ共通コードここまで###

if __name__ == '__main__':
    #####ここからモック用変更####
    # ログ設定
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("connection_job")
    
    # 引数解析
    args = getResolvedOptions(sys.argv, ['YYYYMM'])
    
    # ジョブの起動ログを記録
    print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"Start Glue Job\"}}")
    
    try:
        main()
        # ジョブの終了ログを記録
        print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"End Glue Job\"}}")
        print(f"ジョブが正常に完了しました。出力ディレクトリ: {OUTPUT_DIR}")
    except Exception as e:
        logger.error(f"ジョブ実行中にエラーが発生しました: {str(e)}")
        sys.exit(1)
    ####モック用変更ここまで###
