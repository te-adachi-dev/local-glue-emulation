#!/usr/bin/env python
# -*- coding: utf-8 -*-

#####################################################
# ローカルエミュレーション環境と本番環境の共通モジュール #
#####################################################
import sys
import os
import logging
import pytz
from datetime import datetime
import json
import pandas as pd
from pyspark.sql.functions import col, when, regexp_replace, udf, year, month, dayofmonth, broadcast, to_timestamp
from pyspark.sql.types import TimestampType, StringType

# バケット名管理モジュールをインポート
try:
    import BUCKET_NAME
except ImportError:
    # モジュールが見つからない場合はカレントディレクトリに作成
    from pathlib import Path
    bucket_name_py = Path(__file__).parent / "BUCKET_NAME.py"
    if not bucket_name_py.exists():
        with open(bucket_name_py, "w") as f:
            f.write('''# 本番環境のバケット名
OPEN = "ap-open-bucket"  # 外部公開データ用バケット
MIDDLE = "ap-middle-bucket"  # 中間データ用バケット
MASTER = "ap-master-bucket"  # マスターデータ用バケット
INITIAL = "ap-initial-bucket"  # 初期データ用バケット

# ローカル環境のバケット名（MinIO）
LOCAL_BUCKET = "glue-bucket"
''')
    import BUCKET_NAME

#####################################################
# ローカルエミュレーション環境のみのモジュールとグローバル変数 #
#####################################################
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

# ローカル環境の判定フラグ
IS_LOCAL = os.path.exists('/home/glue_user/workspace')

# ローカル環境用のディレクトリ設定
if IS_LOCAL:
    # コンテナ内での実行
    DATA_DIR = '/home/glue_user/data'
    INPUT_DIR = '/home/glue_user/data/input/csv'
    OUTPUT_DIR = '/home/glue_user/data/output/connection_output'
    COLUMN_MAPPINGS_DIR = '/home/glue_user/crawler/column_mappings'
else:
    # 非ローカル環境（本番環境）用の設定は不要
    DATA_DIR = None
    INPUT_DIR = None
    OUTPUT_DIR = None
    COLUMN_MAPPINGS_DIR = None

# S3/MinIOの設定（ローカル環境用）
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = BUCKET_NAME.LOCAL_BUCKET

# S3クライアントの設定（ローカル環境用）
if IS_LOCAL:
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='ap-northeast-1'
    )
    # 出力ディレクトリの作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# ローカル環境用のモック引数クラス
class MockArgs:
    def __init__(self, yyyymm=None, job_name="connection_job", job_run_id="mock_run_123"):
        self.YYYYMM = yyyymm if yyyymm else "202501"
        self.JOB_NAME = job_name
        self.JOB_RUN_ID = job_run_id

# SparkSessionのモック（S3/MinIO対応）
def create_spark_session(app_name: str) -> SparkSession:
    if IS_LOCAL:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
    else:
        # 本番環境用
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()

# モック用のgetResolvedOptions関数
def getResolvedOptions(argv, args):
    if IS_LOCAL:
        # ローカル環境用
        yyyymm = None
        for i, arg in enumerate(argv):
            if arg == '--YYYYMM' and i+1 < len(argv):
                yyyymm = argv[i+1]
        return MockArgs(yyyymm=yyyymm)
    else:
        # 本番環境用（awsglue.utilsのgetResolvedOptionsが使用される）
        from awsglue.utils import getResolvedOptions as gro
        return gro(argv, args)

#####################################################
# 本番環境とローカル環境の共通関数                    #
#####################################################

# 既存のディレクトリを削除
def delete_existing_directory(dir_path):
    logger.info(" 既存のフォルダ，ファイル削除を開始 ")

    if IS_LOCAL:
        # ローカル環境用のディレクトリ削除
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
    else:
        # 本番環境用のS3ディレクトリ削除
        bucket = BUCKET_NAME.MIDDLE
        path = 'common/common_connection/'
        
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])

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

#####################################################
# メイン処理                                        #
#####################################################
def main():
    # 環境情報のデバッグ出力
    print(f"現在の作業ディレクトリ: {os.getcwd()}")
    if IS_LOCAL:
        print(f"入力ディレクトリ: {INPUT_DIR}")
        print(f"出力ディレクトリ: {OUTPUT_DIR}")
        print(f"S3バケット: {S3_BUCKET}")
        print(f"S3エンドポイント: {S3_ENDPOINT}")

    if IS_LOCAL:
        # S3/MinIOの接続テスト
        try:
            s3_client.list_buckets()
            print(f"S3/MinIO接続テスト成功")
        except Exception as e:
            logger.error(f"S3/MinIO接続テストに失敗しました: {str(e)}")
            sys.exit(1)

        # 入力ファイルの確認
        print("S3/MinIO上の入力ファイルの確認中...")
        
        required_files = [
            ("input/csv/input_connection.csv", "input_connection.csv"),
            ("master/device_attribute/master_device_attribute.csv", "master_device_attribute.csv"),
            ("master/user_attribute/master_user_attribute.csv", "master_user_attribute.csv")
        ]
        
        missing_files = []
        for s3_key, filename in required_files:
            try:
                s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
                print(f"ファイルを確認: s3://{S3_BUCKET}/{s3_key}")
            except:
                missing_files.append(filename)
        
        if missing_files:
            logger.error(f"以下のファイルがS3/MinIOにありません: {', '.join(missing_files)}")
            logger.error("クローラーを実行してファイルをS3/MinIOにアップロードしてください。")
            sys.exit(1)

    # Sparkセッションの作成
    spark = create_spark_session("ConnectionJob")

    # マスターデータの読み込み
    if IS_LOCAL:
        # ローカル環境: MinIO/S3から読み込み
        df_device = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
            .load(f"s3://{S3_BUCKET}/master/device_attribute/master_device_attribute.csv")
        df_device = broadcast(df_device)
        
        df_user = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
            .load(f"s3://{S3_BUCKET}/master/user_attribute/master_user_attribute.csv")
        df_user = broadcast(df_user)

        # input_connectionの読み込み
        input_file = f"s3://{S3_BUCKET}/input/csv/input_connection.csv"
        print(f"入力ファイルパス: {input_file}")
        
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_file)
    else:
        # 本番環境: 実際のS3バケットから読み込み
        df_device = load_csv(spark, f"s3a://{BUCKET_NAME.MASTER}/{BUCKET_NAME.MASTER_DEVICE_PATH}/master_device_attribute.csv")
        df_device = broadcast(df_device)
        df_user = load_csv(spark, f"s3a://{BUCKET_NAME.MASTER}/{BUCKET_NAME.MASTER_USER_PATH}/master_user_attribute.csv")
        df_user = broadcast(df_user)

        # パーケットファイルの読み込み
        parquet_file = f"s3://{BUCKET_NAME.INITIAL}/{BUCKET_NAME.INITIAL_BASE_PATH}/yearmonth={args.YYYYMM}/"
        df = spark.read.option("mergeSchema", "false").parquet(parquet_file)
    
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
    if IS_LOCAL:
        delete_existing_directory(OUTPUT_DIR)
    else:
        # 本番環境のバケットとパスは実際のプロジェクトに合わせて修正
        delete_existing_directory(None)  # 引数は不要

    # 出力前にデータフレーム内にデータが存在することをチェック
    record_count = df.count()
    print(f"出力レコード数: {record_count}")
    if record_count == 0:
        error_message = "出力ファイルにヘッダー以外の情報が含まれていません。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # データの保存
    yearmonth_str = args.YYYYMM
    
    if IS_LOCAL:
        # ローカル環境: ローカルファイルとMinIO/S3に保存
        # パーティション付きで保存
        output_dir = os.path.join(OUTPUT_DIR, f"yearmonth={yearmonth_str}")
        os.makedirs(output_dir, exist_ok=True)

        # データフレームをローカルに保存
        output_path = os.path.join(output_dir, "connection_data.csv")
        df.toPandas().to_csv(output_path, index=False, encoding='utf-8')
        print(f"ファイルをローカルに保存しました: {output_path}")
        
        # S3/MinIOにも保存
        s3_output_path = f"s3://{S3_BUCKET}/output/connection_output/yearmonth={yearmonth_str}"
        df.coalesce(1).write.mode("overwrite").csv(s3_output_path, header=True)
        print(f"ファイルをS3に保存しました: {s3_output_path}")
    else:
        # 本番環境: Parquetとして保存
        logger.info("INFO: ファイル出力します")
        output_parquet_file = f"s3://{BUCKET_NAME.MIDDLE}/common/common_connection/yearmonth={yearmonth_str}"
        df.write.option("compression", "snappy").parquet(output_parquet_file, mode="overwrite")

    logger.info("INFO: ファイル出力が完了しました")

if __name__ == '__main__':
    # ログ設定
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("connection_job")

    # 引数解析
    args = getResolvedOptions(sys.argv, ['YYYYMM'])

    # ジョブの起動ログを記録
    print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"Start Glue Job\"}}")

    #####################################################
    # 本番環境のオリジナルコード (コメントアウト)           #
    #####################################################
    """
    # 以下は本番環境のオリジナルコード
    import sys
    import BUCKET_NAME
    import boto3
    import logging
    import pytz
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, regexp_replace, udf, year, month, dayofmonth, broadcast, to_timestamp
    from pyspark.sql.types import TimestampType, StringType
    from datetime import datetime
    from pyspark.sql.dataframe import DataFrame
    from awsglue.utils import getResolvedOptions
    import json

    # ログのフォーマットを指定
    log_format = '{asctime} Level:{levelname}, Message:"{message}"'
    logging.basicConfig(format=log_format, style='{', level=logging.INFO)
    logger = logging.getLogger(__name__)

    #処理対象月を取得
    args = getResolvedOptions(sys.argv, ['YYYYMM'])
    yyyymm = args['YYYYMM']

    # S3バケットとパスの設定
    initial_bucket_name = BUCKET_NAME.INITIAL
    initial_base_path = 'input/input_connection/'

    master_bucket_name = BUCKET_NAME.MASTER
    master_device_path = 'device_attribute/'
    master_user_path = 'user_attribute/'

    middle_bucket_name = BUCKET_NAME.MIDDLE
    middle_base_path = 'common/common_connection/'

    # S3クライアントの作成
    s3 = boto3.client('s3')

    # データバリデーションチェック
    required_list = ['APID']

    input_column_list = ["APID", "利用開始日時", "利用者属性", "端末属性", "事業者"]

    # SparkSessionの初期化
    def create_spark_session(app_name: str) -> SparkSession:
        return SparkSession.builder \\
            .appName(app_name) \\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \\
            .getOrCreate()

    # 既存のディレクトリを削除
    def delete_existing_directory(bucket, path):
        logger.info("*** 既存のフォルダ，ファイル削除を開始 ***")
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])

    # ファイルの読み込み
    def load_csv(spark: SparkSession, file_path: str) -> DataFrame:
        return spark.read.csv(file_path, header=True, inferSchema=True)

    # 属性置換処理
    def replace_attributes(df, df_user, df_device):
        logger.info("*** 属性置換処理を開始 ***")
        
        # 利用者属性の置換
        df = df.join(df_user, df["利用者属性"] == df_user["各社利用者属性"], "left") \\
               .withColumn("利用者属性", when(col("共通ログ利用者属性").isNotNull(), col("共通ログ利用者属性")).otherwise("その他")) \\
               .drop("各社利用者属性", "日本語表記", "共通ログ利用者属性")

        # 端末属性の置換
        df = df.join(df_device, df["端末属性"] == df_device["各社端末属性"], "left") \\
               .withColumn("端末属性", when(col("共通ログ端末属性").isNotNull(), col("共通ログ端末属性")).otherwise("その他")) \\
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
        logger.info("*** 日付処理を開始 ***")
        
        # UDFは処理が重いので，to_timestampで代替
        df = df.withColumn("利用開始日時", to_timestamp(col("利用開始日時"), "yyyy-MM-dd HH:mm:ss"))

        # 年月 (yyyymm形式) と 日 (dd) のカラムを抽出、NULLの場合はNoneを設定
        df = df.withColumn("年月", when(col("利用開始日時").isNotNull(), (year(col("利用開始日時")) * 100 + month(col("利用開始日時")))).otherwise(None))
        df = df.withColumn("日", when(col("利用開始日時").isNotNull(), dayofmonth(col("利用開始日時"))).otherwise(None))
        
        # 利用開始日時をstringとして扱う
        df = df.withColumn("利用開始日時", col("利用開始日時").cast(StringType()))

        return df

    def main():
        # Sparkセッションの作成
        spark = create_spark_session("ReadParquetFromS3")

        # マスターデータの読み込み
        df_device = load_csv(spark, f"s3a://{master_bucket_name}/{master_device_path}/master_device_attribute.csv")
        df_device = broadcast(df_device)
        df_user = load_csv(spark, f"s3a://{master_bucket_name}/{master_user_path}/master_user_attribute.csv")
        df_user = broadcast(df_user)

        # パーケットファイルの読み込み
        # parquet_file = "perfomance_connection_nttbp_202512.parquet"
        parquet_file = f"s3://{initial_bucket_name}/{initial_base_path}/yearmonth={yyyymm}/"
        df = spark.read.option("mergeSchema", "false").parquet(parquet_file)
        
        # 必須カラムチェック（フォーマットチェック）
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
        if not df.take(1):
            # エラーログの出力
            error_message = "データ件数は0件です。データ内容を確認してください。"
            log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args['JOB_RUN_ID']}, Level:ERROR, Message:\"{error_message}\"}}"
            logger.error(log_entry)
            sys.exit(1)
        
        # 'APID'がない時にドロップ
        df_null_apid = df.filter(col("APID").isNull())
        # if not df_null_apid.isEmpty():
        #     logger.info(f"APIDがnullの行が{null_apid_count}件あります。")
        #     df_null_apid.show(truncate=False)

        # nullの行が存在するかどうかを確認
        if not df_null_apid.isEmpty():
            error_message = "このレコードの形式が不正です。"
            log = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args['JOB_RUN_ID']}, Level:WARN, Message:\"{error_message}\""
            data = df_null_apid.rdd.map(lambda row: row.asDict()).collect()
            log_output = log + ", data:" + ",".join([json.dumps(d, ensure_ascii=False) for d in data]) + "}"
            logger.warning(log_output)
        
        df = df.na.drop(subset=required_list)
        
        # データフレームのレコードが0件かどうかのチェックを実施
        # if not df.take(1):
        #     # エラーログの出力
        #     error_message = "データ件数は0件です。データ内容を確認してください。"
        #     log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args['JOB_RUN_ID']}, Level:ERROR, Message:\"{error_message}\"}}"
        #     logger.error(log_entry)
        #     sys.exit(1)

        # 属性置換
        df = replace_attributes(df, df_user, df_device)

        # 日付処理
        df = process_dates(df)

        # 必要なカラムを設定
        df = df.select("管理ID", "APID", "利用開始日時", "利用者属性", "端末属性", "年月", "日", "事業者")

        # 既存のディレクトリを削除
        delete_existing_directory(middle_bucket_name, middle_base_path)
        
        # 出力前にデータフレーム内にデータが存在することをチェック
        if df.isEmpty():
            error_message = "出力ファイルにヘッダー以外の情報が含まれていません。"
            log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args['JOB_RUN_ID']}, Level:ERROR, Message:\"{error_message}\"}}"
            logger.error(log_entry)
            sys.exit(1)

        # Parquetに保存
        logger.info("INFO: ファイル出力します")
        output_parquet_file = f"s3://{middle_bucket_name}/{middle_base_path}/yearmonth={yyyymm}"
        df.write.option("compression", "snappy").parquet(output_parquet_file, mode="overwrite")

        # 結果の表示
        # df.show(truncate=False)
        # df.printSchema()
        
        
    if __name__ == '__main__':
        
        #ジョブの起動ログを記録
        print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args['JOB_RUN_ID']}, Level:INFO, Message:\"Start Glue Job\"}}")
        
        main()
        
        #ジョブの終了ログを記録
        print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args['JOB_RUN_ID']}, Level:INFO, Message:\"End Glue Job\"}}")
    """

    try:
        main()
        # ジョブの終了ログを記録
        print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"End Glue Job\"}}")
        
        # ローカル環境用の追加情報
        if IS_LOCAL:
            print(f"ジョブが正常に完了しました。出力ディレクトリ: {OUTPUT_DIR}")
    except Exception as e:
        logger.error(f"ジョブ実行中にエラーが発生しました: {str(e)}")
        sys.exit(1)
