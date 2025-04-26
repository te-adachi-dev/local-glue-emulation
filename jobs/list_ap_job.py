#!/usr/bin/env python
# -*- coding: utf-8 -*-

#####ここからモック用変更####
import sys
import os
import pandas as pd
import json
import re
import requests
from datetime import datetime
import pytz
from functools import reduce
import logging
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, when, sum as _sum
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

# モック環境用の設定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data")
INPUT_DIR = os.path.join(DATA_DIR, "input", "csv")
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "list_ap_output")
COLUMN_MAPPINGS_DIR = os.path.join(os.path.dirname(BASE_DIR), "crawler", "column_mappings")

# 出力ディレクトリの作成
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 引数処理のモック
class MockArgs:
    def __init__(self, yyyymm, job_name="list_ap_job", job_run_id="mock_run_123"):
        self.YYYYMM = yyyymm
        self.JOB_NAME = job_name
        self.JOB_RUN_ID = job_run_id

# AWS Glue関連のモッククラス
class DynamicFrame:
    def __init__(self, dataframe, context, name):
        self.dataframe = dataframe
        self.context = context
        self.name = name
    
    def toDF(self):
        return self.dataframe
    
    @classmethod
    def fromDF(cls, dataframe, context, name):
        return cls(dataframe, context, name)

class DynamicFrameCollection:
    def __init__(self, frames, context):
        self.frames = frames
        self.context = context
    
    def keys(self):
        return list(self.frames.keys())
    
    def select(self, key):
        return self.frames[key]

class Job:
    def __init__(self, context):
        self.context = context
    
    def init(self, name, args):
        self.name = name
        self.args = args
    
    def commit(self):
        print(f"Job {self.name} committed")

class GlueContext:
    def __init__(self, spark_context):
        self.spark_context = spark_context
        self.spark_session = spark_context.getOrCreate()
    
    def create_dynamic_frame_from_catalog(self, database, table_name, transformation_ctx):
        # Hiveメタストアの代わりにCSVファイルから読み込む
        if table_name == "input_ap":
            file_path = os.path.join(INPUT_DIR, f"{table_name}.csv")
            df = self.spark_session.read.csv(file_path, header=True, inferSchema=True)
            return DynamicFrame(df, self, transformation_ctx)
    
    def create_dynamic_frame_from_options(self, format_options, connection_type, format, connection_options, transformation_ctx):
        # マスターデータのCSVを読み込む
        if "master_status.csv" in str(connection_options):
            file_path = os.path.join(INPUT_DIR, "master_status.csv")
            df = self.spark_session.read.csv(file_path, header=True, inferSchema=True)
            return DynamicFrame(df, self, transformation_ctx)
        return None
    
    def write_dynamic_frame_from_options(self, frame, connection_type, format, connection_options, format_options=None):
        # パーケットファイルの代わりにCSVとして保存
        df = frame.dataframe
        output_path = os.path.join(OUTPUT_DIR, "common_ap")
        os.makedirs(output_path, exist_ok=True)
        
        # パーティションキーがある場合は分割して保存
        if "partitionKeys" in connection_options:
            partition_keys = connection_options["partitionKeys"]
            for key in partition_keys:
                df.write.partitionBy(key).csv(os.path.join(output_path, key), header=True, mode="overwrite")
        else:
            df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

# モック用のgetResolvedOptions関数
def getResolvedOptions(argv, args):
    # コマンドライン引数からパラメータを取得するモック
    # 実際の環境では適切に引数を解析する必要がある
    return MockArgs("202501")

# SelectFromCollectionのモック
class SelectFromCollection:
    @staticmethod
    def apply(dfc, key, transformation_ctx):
        return dfc.select(key)

# ApplyMappingのモック
class ApplyMapping:
    @staticmethod
    def apply(frame, mappings, transformation_ctx):
        df = frame.toDF()
        # カラム名のマッピングを適用
        for src_col, src_type, tgt_col, tgt_type in mappings:
            if src_col in df.columns:
                df = df.withColumnRenamed(src_col, tgt_col)
        return DynamicFrame(df, frame.context, transformation_ctx)
####モック用変更ここまで###

#####本物Ｇｌｕｅと共通コード####
# Exception Handling2
def ExceptionHandling2(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameCollectionからDynamicFrameを取得
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    spark_df = dynamic_frame.toDF()

    # 空白の文字列をNULLに変換
    spark_df = spark_df.replace("", None)

    # データバリデーションチェック
    required_list = ['APID', 'AP名称', '設置場所名称', '都道府県', '市区町村', '住所', '緯度', '経度', 'ステータス']

    error_message = "このレコードの形式が不正です。"
    log = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:WARN, Message:\"{error_message}\""

    # データフレーム全体のnull値をチェック
    null_values = spark_df.select([col(c).isNull().alias(c) for c in required_list]).agg(*[_sum(col(c).cast("int")).alias(c) for c in required_list])

    if any(null_values.collect()[0]):
        log_output = log + ", data:"

        # null値を含む個別の行を収集
        null_rows = spark_df.filter(reduce(lambda x, y: x | y, [col(c).isNull() for c in required_list]))
        data = null_rows.rdd.map(lambda row: row.asDict()).collect()
        log_output += ",".join([str(d) for d in data]) + "}"
        logger.warn(log_output)

    spark_df = spark_df.na.drop(subset=required_list)

    # データ件数のチェック
    if spark_df.count() == 0:
        error_message = "出力ファイルにヘッダー以外の情報が含まれていません。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # Spark DataFrameをDynamicFrameに変換
    transformed_dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "transformed_dynamic_frame")

    # DynamicFrameをDynamicFrameCollectionに変換して返す
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_dynamic_frame}, glueContext)

# Conversion Process
def ConversionProcess(glueContext, dfc) -> DynamicFrameCollection:
    import datetime as dt
    # DynamicFrameCollectionからDynamicFrameを取得
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    df = dynamic_frame.toDF()

    # 緯度・経度の保持
    lat_lon_keep = {}

    # 都道府県、市区町村、住所から緯度・経度を取得
    def get_lat_lon(prefecture, city, address):
        # すでに緯度・経度がある場合はそのまま返す
        if prefecture is None or city is None or address is None:
            return None, None

        key = f"{prefecture}{city}{address}"
        if key in lat_lon_keep:
            return lat_lon_keep[key]

        url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={prefecture}{city}{address}"
        try:
            response = requests.get(url, timeout=30)  # タイムアウトを30秒に設定
            if response.status_code == 200:
                data = response.json()
                if data:
                    lat_lon_keep[key] = (data[0]['geometry']['coordinates'][1], data[0]['geometry']['coordinates'][0])
                    return lat_lon_keep[key]
        except Exception as e:
            print(f"Request failed: {e}")  # その他のリクエストエラーを処理
            return None, None  # エラーが発生した場合もNoneを返す
        return None, None

    # 緯度と経度を取得するUDF
    def get_lat_lon_udf(prefecture, city, address, lat, lon):
        if lat is None or lon is None:
            lat, lon = get_lat_lon(prefecture, city, address)
        return (lat, lon)

    schema = StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])

    get_lat_lon_combined = udf(lambda prefecture, city, address, lat, lon: get_lat_lon_udf(prefecture, city, address, lat, lon), schema)

    df = df.withColumn("lat_lon", get_lat_lon_combined(col("都道府県"), col("市区町村"), col("住所"), col("緯度"), col("経度")))
    df = df.withColumn("緯度", col("lat_lon").getItem("lat"))
    df = df.withColumn("経度", col("lat_lon").getItem("lon"))
    df = df.drop("lat_lon")

    # 日付をISO8601形式に変換するUDFを定義
    def convert_to_iso8601(date_str):
        if date_str is None:
            return None

        date_formats = [
            "%Y/%m/%d",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S"
        ]

        for fmt in date_formats:
            try:
                return dt.datetime.strptime(date_str, fmt).isoformat()
            except (ValueError, TypeError):
                continue
        return None

    convert_to_iso8601_udf = udf(convert_to_iso8601, StringType())

    # 日付をISO8601形式に変換
    df = df.withColumn("利用開始日時", convert_to_iso8601_udf(col("利用開始日時")))
    df = df.withColumn("利用終了日時", convert_to_iso8601_udf(col("利用終了日時")))

    # DataFrameをDynamicFrameに戻す
    transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

    # DynamicFrameをDynamicFrameCollectionに変換して返す
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_dynamic_frame}, glueContext)

# output_list_ap_backup
def output_list_ap_backup(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameをDataFrameに変換
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # パーティションを1つにまとめる
    df = df.coalesce(1)

    # DataFrame保存関数
    def save_dataframe_to_file(data_frame, file_path, file_name):
        # 出力ディレクトリの作成
        os.makedirs(file_path, exist_ok=True)
        
        # CSVとして保存
        full_path = os.path.join(file_path, file_name)
        data_frame.toPandas().to_csv(full_path, index=False, encoding='utf-8')
        print(f"ファイルを保存しました: {full_path}")

    # DataFrameをDynamicFrameに戻す
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # ファイルに保存
    file_path = os.path.join(OUTPUT_DIR, "list_ap_backup")
    file_name = f'backup_listap{args.YYYYMM}.csv'
    save_dataframe_to_file(df, file_path, file_name)

    return DynamicFrameCollection({"output": dynamic_frame}, glueContext)

# Exception Handling1
def ExceptionHandling1(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameCollectionからDynamicFrameを取得
    dynamic_frame = dfc.select(list(dfc.keys())[0])

    # DataFrameに変換
    df = dynamic_frame.toDF()

    # データ件数のチェック
    if df.count() == 0:
        # エラーログの出力
        error_message = "データ件数は0件です。データ内容を確認してください。"
        log_entry = f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        logger.error(log_entry)
        sys.exit(1)

    # DataFrameをDynamicFrameに戻す
    transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

    # DynamicFrameをDynamicFrameCollectionに変換して返す
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_dynamic_frame}, glueContext)

# sparkSqlQuery
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx):
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# output_list_ap_table
def output_list_ap_table(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameをDataFrameに変換
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # パーティションを1つにまとめる
    df = df.coalesce(1)

    # DataFrame保存関数
    def save_dataframe_to_file(data_frame, file_path, file_name):
        # 出力ディレクトリの作成
        os.makedirs(file_path, exist_ok=True)
        
        # CSVとして保存
        full_path = os.path.join(file_path, file_name)
        data_frame.toPandas().to_csv(full_path, index=False, encoding='utf-8')
        print(f"ファイルを保存しました: {full_path}")

    # DataFrameをDynamicFrameに戻す
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # ファイルに保存
    file_path = os.path.join(OUTPUT_DIR, "list_ap_table")
    file_name = f'listap{args.YYYYMM}.csv'
    save_dataframe_to_file(df, file_path, file_name)

    return DynamicFrameCollection({"output": dynamic_frame}, glueContext)

# ProcessMonth
def ProcessMonth(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameCollectionからDynamicFrameを取得
    dynamic_frame = dfc.select(list(dfc.keys())[0])

    # DynamicFrameをDataFrameに変換
    df = dynamic_frame.toDF()

    # 格納データから処理対象月データのみを抽出
    df_filtered = df.filter(col("yearmonth") == args.YYYYMM)

    # DataFrameをDynamicFrameに戻す
    transformed_dynamic_frame = DynamicFrame.fromDF(df_filtered, glueContext, "transformed_dynamic_frame")

    # DynamicFrameをDynamicFrameCollectionに変換して返す
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_dynamic_frame}, glueContext)

# output_common_ap
def output_common_ap(glueContext, dfc) -> DynamicFrameCollection:
    # DynamicFrameをDataFrameに変換
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # パーティション追加用の列を追加
    df = df.withColumn("yearmonth", df["年月"])

    # DataFrameをDynamicFrameに戻す
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # 出力ディレクトリの設定
    output_dir = os.path.join(OUTPUT_DIR, "common_ap")
    os.makedirs(output_dir, exist_ok=True)

    # 既存のファイルを削除
    for file in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    # パーケット形式で保存（モック環境ではCSVに変換）
    glueContext.write_dynamic_frame_from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="parquet",
        connection_options={"path": output_dir, "partitionKeys": ["yearmonth"]},
        format_options={"compression": "uncompressed"}
    )

    return DynamicFrameCollection({"output": dynamic_frame}, glueContext)
####本物Ｇｌｕｅ共通コードここまで###

# メイン処理
if __name__ == "__main__":
    #####ここからモック用変更####
    # ログ設定
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("list_ap_job")
    
    # 引数解析
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YYYYMM'])
    
    # Sparkコンテキスト設定
    spark = SparkSession.builder.appName("list_ap_job").getOrCreate()
    sc = spark.sparkContext
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args.JOB_NAME, args)
    
    # 入力ファイルパスを調整
    # CSVファイルの存在確認と名前の整合性を確保
    input_files = {}
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            # ファイル名からテーブル名を抽出（拡張子を除去）
            table_name = os.path.splitext(file)[0]
            input_files[table_name] = os.path.join(INPUT_DIR, file)
    
    # 必要なデータの存在チェック
    required_files = ["input_ap", "master_status"]
    for req_file in required_files:
        if not any(req_file in file for file in input_files):
            logger.error(f"必要なファイル {req_file}.csv が見つかりません")
            sys.exit(1)
    
    # カラムマッピング情報の読み込み（必要な場合）
    column_mappings = {}
    if os.path.exists(COLUMN_MAPPINGS_DIR):
        for mapping_file in os.listdir(COLUMN_MAPPINGS_DIR):
            if mapping_file.endswith("_mapping.csv"):
                table_name = mapping_file.replace("_mapping.csv", "")
                mapping_path = os.path.join(COLUMN_MAPPINGS_DIR, mapping_file)
                column_mappings[table_name] = pd.read_csv(mapping_path)
    ####モック用変更ここまで###
    
    #####本物Ｇｌｕｅと共通コード####
    # ジョブの起動ログを記録
    print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"Start Glue Job\"}}")

    # Script generated for node ap
    ap_node = glueContext.create_dynamic_frame_from_catalog(database="input", table_name="input_ap", transformation_ctx="ap_node")

    # Script generated for node status_master
    status_master_node = glueContext.create_dynamic_frame_from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
        connection_type="s3",
        format="csv",
        connection_options={"paths": ["master_status.csv"], "recurse": True},
        transformation_ctx="status_master_node"
    )

    # Script generated for node Process Month
    ProcessMonth_node = ProcessMonth(glueContext, DynamicFrameCollection({"ap_node": ap_node}, glueContext))

    # Script generated for node Select From Collection
    SelectFromCollection_node1 = SelectFromCollection.apply(dfc=ProcessMonth_node, key=list(ProcessMonth_node.keys())[0], transformation_ctx="SelectFromCollection_node1")

    # Script generated for node Change Schema
    ChangeSchema_node1 = ApplyMapping.apply(frame=SelectFromCollection_node1, mappings=[
        ("APID", "string", "APID", "string"),
        ("AP名称", "string", "AP名称", "string"),
        ("設置場所名称", "string", "設置場所名称", "string"),
        ("都道府県", "string", "都道府県", "string"),
        ("市区町村", "string", "市区町村", "string"),
        ("住所", "string", "住所", "string"),
        ("補足情報", "string", "補足情報", "string"),
        ("緯度", "double", "緯度", "double"),
        ("経度", "double", "経度", "double"),
        ("ステータス", "string", "ステータス", "string"),
        ("利用開始日時", "string", "利用開始日時", "string"),
        ("利用終了日時", "string", "利用終了日時", "string"),
        ("カテゴリ", "string", "カテゴリ", "string"),
        ("保有主体属性", "string", "保有主体属性", "string"),
        ("施設属性", "string", "施設属性", "string"),
        ("局属性", "string", "局属性", "string"),
        ("事業者", "string", "事業者", "string"),
        ("yearmonth", "string", "yearmonth", "string")
    ], transformation_ctx="ChangeSchema_node1")

    # Script generated for node Exception Handling1
    ExceptionHandling1_node = ExceptionHandling1(glueContext, DynamicFrameCollection({"ChangeSchema_node1": ChangeSchema_node1}, glueContext))

    # Script generated for node Select From Collection
    SelectFromCollection_node2 = SelectFromCollection.apply(dfc=ExceptionHandling1_node, key=list(ExceptionHandling1_node.keys())[0], transformation_ctx="SelectFromCollection_node2")

    # Script generated for node Conversion Process
    ConversionProcess_node = ConversionProcess(glueContext, DynamicFrameCollection({"SelectFromCollection_node2": SelectFromCollection_node2}, glueContext))

    # Script generated for node Select From Collection
    SelectFromCollection_node3 = SelectFromCollection.apply(dfc=ConversionProcess_node, key=list(ConversionProcess_node.keys())[0], transformation_ctx="SelectFromCollection_node3")

    # Script generated for node Translate ap_ID & Status unification
    SqlQuery0 = '''
    select 
        case 
            when apid rlike '([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}' then 
                regexp_replace(apid, '[-:]', '') 
            else 
                apid 
        end as 管理ID,
        apid as APID,
        ap名称 as AP名称,
        設置場所名称,
        都道府県,
        市区町村,
        住所,
        補足情報,
        緯度,
        経度,
        COALESCE(master_status.共通ログステータス, 'その他') AS ステータス,
        利用開始日時,
        利用終了日時,
        カテゴリ,
        保有主体属性,
        施設属性,
        局属性,
        yearmonth as 年月,
        事業者
    from ap_status
    left outer join master_status
    on ap_status.ステータス = master_status.各社ステータス
    '''
    Translateap_IDStatusunification_node = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={"ap_status": SelectFromCollection_node3, "master_status": status_master_node},
        transformation_ctx="Translateap_IDStatusunification_node"
    )

    # Script generated for node Exception Handling2
    ExceptionHandling2_node = ExceptionHandling2(glueContext, DynamicFrameCollection({"Translateap_IDStatusunification_node": Translateap_IDStatusunification_node}, glueContext))

    # Script generated for node Select From Collection
    SelectFromCollection_node4 = SelectFromCollection.apply(dfc=ExceptionHandling2_node, key=list(ExceptionHandling2_node.keys())[0], transformation_ctx="SelectFromCollection_node4")

    # Script generated for node Change Schema
    ChangeSchema_node2 = ApplyMapping.apply(frame=SelectFromCollection_node4, mappings=[
        ("管理ID", "string", "管理ID", "string"),
        ("APID", "string", "APID", "string"),
        ("AP名称", "string", "AP名称", "string"),
        ("設置場所名称", "string", "設置場所名称", "string"),
        ("都道府県", "string", "都道府県", "string"),
        ("市区町村", "string", "市区町村", "string"),
        ("住所", "string", "住所", "string"),
        ("補足情報", "string", "補足情報", "string"),
        ("緯度", "double", "緯度", "double"),
        ("経度", "double", "経度", "double"),
        ("ステータス", "string", "ステータス", "string"),
        ("利用開始日時", "string", "利用開始日時", "string"),
        ("利用終了日時", "string", "利用終了日時", "string"),
        ("カテゴリ", "string", "カテゴリ", "string"),
        ("保有主体属性", "string", "保有主体属性", "string"),
        ("施設属性", "string", "施設属性", "string"),
        ("局属性", "string", "局属性", "string"),
        ("年月", "string", "年月", "int"),
        ("事業者", "string", "事業者", "string")
    ], transformation_ctx="ChangeSchema_node2")

    # Script generated for node list_ap_table d2
    list_ap_tabled2_node = output_list_ap_table(glueContext, DynamicFrameCollection({"ChangeSchema_node2": ChangeSchema_node2}, glueContext))

    # Script generated for node common_ap d2
    common_apd2_node = output_common_ap(glueContext, DynamicFrameCollection({"ChangeSchema_node2": ChangeSchema_node2}, glueContext))

    # Script generated for node list_ap_backup d1
    list_ap_backupd1_node = output_list_ap_backup(glueContext, DynamicFrameCollection({"ChangeSchema_node2": ChangeSchema_node2}, glueContext))

    # ジョブの終了ログを記録
    print(f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"End Glue Job\"}}")
    ####本物Ｇｌｕｅ共通コードここまで###

    #####ここからモック用変更####
    # ジョブのコミット
    job.commit()
    
    # 出力結果の表示
    print(f"ジョブが正常に完了しました。出力ディレクトリ: {OUTPUT_DIR}")
    ####モック用変更ここまで###
