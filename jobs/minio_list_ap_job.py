#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AWS Glue ETL script for processing AP (Access Point) data.

This script handles data transformation, validation, and output generation
for AP-related datasets, supporting both local and production environments.
It integrates with S3/MinIO for data storage and uses PySpark for data processing.
"""

import logging
import os
import sys
from datetime import datetime
from functools import reduce
from pathlib import Path

import boto3
import pandas as pd
import pytz
import requests
from botocore.client import Config
from pyspark.context import SparkContext
from pyspark.sql.functions import col, regexp_replace, sum as spark_sum, udf, when
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

# Attempt to import BUCKET_NAME; if not found, create a default configuration
try:
    import BUCKET_NAME
except ImportError:
    bucket_name_path = Path(__file__).parent / "BUCKET_NAME.py"
    if not bucket_name_path.exists():
        with open(bucket_name_path, "w", encoding="utf-8") as f:
            f.write(
                """# Production bucket names
OPEN = "ap-open-bucket"  # Bucket for public data
MIDDLE = "ap-middle-bucket"  # Bucket for intermediate data
MASTER = "ap-master-bucket"  # Bucket for master data
INITIAL = "ap-initial-bucket"  # Bucket for initial data
LOCAL_BUCKET = "glue-bucket"  # Local MinIO bucket
"""
            )
    import BUCKET_NAME

# Environment detection
IS_LOCAL = os.path.exists("/home/glue_user/workspace")

# Directory and S3 configurations
if IS_LOCAL:
    DATA_DIR = "/home/glue_user/data"
    INPUT_DIR = os.path.join(DATA_DIR, "input/csv")
    OUTPUT_DIR = os.path.join(DATA_DIR, "output/list_ap_output")
    COLUMN_MAPPINGS_DIR = "/home/glue_user/crawler/column_mappings"
    S3_BUCKET = BUCKET_NAME.LOCAL_BUCKET
    S3_ENDPOINT = "http://minio:9000"
    S3_ACCESS_KEY = "minioadmin"
    S3_SECRET_KEY = "minioadmin"
else:
    DATA_DIR = None
    INPUT_DIR = None
    OUTPUT_DIR = None
    COLUMN_MAPPINGS_DIR = None
    S3_BUCKET = None
    S3_ENDPOINT = None
    S3_ACCESS_KEY = None
    S3_SECRET_KEY = None

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("list_ap_job")


def get_s3_client():
    """Initialize and return an S3 client for local or production use."""
    endpoint_url = os.environ.get("MINIO_ENDPOINT", S3_ENDPOINT)
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
    region_name = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=region_name,
    )


# Initialize S3 client for local environment
if IS_LOCAL:
    s3_client = get_s3_client()
    os.makedirs(OUTPUT_DIR, exist_ok=True)


class MockArgs:
    """Mock arguments for local testing."""

    def __init__(self, yyyymm=None, job_name="list_ap_job", job_run_id="mock_run_123"):
        self.YYYYMM = yyyymm or "202501"
        self.JOB_NAME = job_name
        self.JOB_RUN_ID = job_run_id


class DynamicFrame:
    """Wrapper for PySpark DataFrame to emulate AWS Glue DynamicFrame."""

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
    """Collection of DynamicFrames for processing multiple frames."""

    def __init__(self, frames, context):
        self.frames = frames
        self.context = context

    def keys(self):
        return list(self.frames.keys())

    def select(self, key):
        return self.frames[key]


class Job:
    """Emulates AWS Glue Job for managing job lifecycle."""

    def __init__(self, context):
        self.context = context

    def init(self, name, args):
        self.name = name
        self.args = args

    def commit(self):
        logger.info(f"Job {self.name} committed")


class GlueContext:
    """Emulates AWS Glue Context for data operations."""

    def __init__(self, spark_context):
        self.spark_context = spark_context
        # ここを修正: SparkSessionを正しく取得する
        self.spark_session = SparkSession.builder.getOrCreate()

    def create_dynamic_frame_from_catalog(self, database, table_name, transformation_ctx):
        """Load data from catalog (S3 for local environment)."""
        if IS_LOCAL and table_name == "input_ap":
            file_path = f"s3://{S3_BUCKET}/input/csv/{table_name}.csv"
            logger.info(f"Reading from S3: {file_path}")
            df = (
                self.spark_session.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(file_path)
            )
            return DynamicFrame(df, self, transformation_ctx)
        raise NotImplementedError("Production environment not supported locally")

    def create_dynamic_frame_from_options(
        self, format_options, connection_type, format, connection_options, transformation_ctx
    ):
        """Load data from specified options (S3 for local environment)."""
        if IS_LOCAL and "paths" in connection_options:
            path = connection_options["paths"][0]
            if "master_status.csv" in path:
                file_path = f"s3://{S3_BUCKET}/master/status/master_status.csv"
                logger.info(f"Reading from S3: {file_path}")
                df = (
                    self.spark_session.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(file_path)
                )
                return DynamicFrame(df, self, transformation_ctx)
        raise NotImplementedError("Production environment not supported locally")

    def write_dynamic_frame_from_options(
        self, frame, connection_type, format, connection_options, format_options=None
    ):
        """Write DynamicFrame to S3 or local storage."""
        if IS_LOCAL:
            df = frame.dataframe
            if "path" in connection_options:
                output_path = None
                base_path = connection_options["path"]

                if "/common_ap" in base_path:
                    output_path = f"s3://{S3_BUCKET}/output/list_ap_output/common_ap"

                if output_path:
                    logger.info(f"Writing to S3: {output_path}")
                    if "partitionKeys" in connection_options:
                        partition_keys = connection_options["partitionKeys"]
                        df.write.partitionBy(*partition_keys).mode("overwrite").format(
                            format
                        ).save(output_path)
                    else:
                        df.coalesce(1).write.mode("overwrite").format(format).save(
                            output_path
                        )

                    local_output_path = os.path.join(OUTPUT_DIR, "common_ap")
                    os.makedirs(local_output_path, exist_ok=True)

                    if "partitionKeys" in connection_options:
                        for key in connection_options["partitionKeys"]:
                            partition_values = df.select(key).distinct().collect()
                            for row in partition_values:
                                partition_value = row[key]
                                partition_path = os.path.join(
                                    local_output_path, f"{key}/{key}={partition_value}"
                                )
                                os.makedirs(partition_path, exist_ok=True)
                                filtered_df = df.filter(col(key) == partition_value)
                                filtered_df.toPandas().to_csv(
                                    os.path.join(partition_path, "part-00000.csv"),
                                    index=False,
                                )
                    else:
                        df.toPandas().to_csv(
                            os.path.join(local_output_path, "data.csv"), index=False
                        )
        else:
            raise NotImplementedError("Production environment not supported locally")

    def write_dynamic_frame(self):
        """Return a writer object for DynamicFrame."""
        class FromOptions:
            def __init__(self, glue_ctx):
                self.glue_ctx = glue_ctx

            def from_options(
                self, frame, connection_type, format, connection_options, format_options=None
            ):
                self.glue_ctx.write_dynamic_frame_from_options(
                    frame=frame,
                    connection_type=connection_type,
                    format=format,
                    connection_options=connection_options,
                    format_options=format_options,
                )

        return FromOptions(self)


def get_resolved_options(argv, args):
    """Parse command-line arguments for Glue job."""
    if IS_LOCAL:
        yyyymm = None
        job_name = "list_ap_job"
        for i, arg in enumerate(argv):
            if arg == "--YYYYMM" and i + 1 < len(argv):
                yyyymm = argv[i + 1]
            elif arg == "--JOB_NAME" and i + 1 < len(argv):
                job_name = argv[i + 1]
        return MockArgs(yyyymm=yyyymm, job_name=job_name)
    from awsglue.utils import getResolvedOptions

    return getResolvedOptions(argv, args)


class SelectFromCollection:
    """Utility to select a frame from a DynamicFrameCollection."""

    @staticmethod
    def apply(dfc, key, transformation_ctx):
        return dfc.select(key)


class ApplyMapping:
    """Utility to apply column mappings to a DynamicFrame."""

    @staticmethod
    def apply(frame, mappings, transformation_ctx):
        df = frame.toDF()
        for src_col, _, tgt_col, _ in mappings:
            if src_col in df.columns:
                df = df.withColumnRenamed(src_col, tgt_col)
        return DynamicFrame(df, frame.context, transformation_ctx)


def process_month(glue_context, dfc):
    """Filter data by specified year-month."""
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    df = dynamic_frame.toDF().filter(col("yearmonth") == args.YYYYMM)
    transformed_frame = DynamicFrame.fromDF(df, glue_context, "transformed_dynamic_frame")
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_frame}, glue_context)


def exception_handling_1(glue_context, dfc):
    """Check if DataFrame is empty and log error if so."""
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    df = dynamic_frame.toDF()

    if df.count() == 0:
        error_message = "データ件数は0件です。データ内容を確認してください。"
        log_entry = (
            f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, "
            f"jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        )
        logger.error(log_entry)
        sys.exit(1)

    transformed_frame = DynamicFrame.fromDF(df, glue_context, "transformed_dynamic_frame")
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_frame}, glue_context)


def exception_handling_2(glue_context, dfc):
    """Validate required columns and remove null rows."""
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    df = dynamic_frame.toDF().replace("", None)

    required_columns = [
        "APID",
        "AP名称",
        "設置場所名称",
        "都道府県",
        "市区町村",
        "住所",
        "緯度",
        "経度",
        "ステータス",
    ]

    null_counts = df.select(
        [col(c).isNull().alias(c) for c in required_columns]
    ).agg(*[spark_sum(col(c).cast("int")).alias(c) for c in required_columns])

    if any(null_counts.collect()[0]):
        error_message = "このレコードの形式が不正です。"
        log = (
            f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, "
            f"jobID:{args.JOB_RUN_ID}, Level:WARN, Message:\"{error_message}\""
        )
        null_rows = df.filter(
            reduce(lambda x, y: x | y, [col(c).isNull() for c in required_columns])
        )
        data = null_rows.rdd.map(lambda row: row.asDict()).collect()
        log_output = f"{log}, data:{','.join([str(d) for d in data])}}}"
        logger.warn(log_output)

    df = df.na.drop(subset=required_columns)

    if df.count() == 0:
        error_message = "出力ファイルにヘッダー以外の情報が含まれていません。"
        log_entry = (
            f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, "
            f"jobID:{args.JOB_RUN_ID}, Level:ERROR, Message:\"{error_message}\"}}"
        )
        logger.error(log_entry)
        sys.exit(1)

    transformed_frame = DynamicFrame.fromDF(df, glue_context, "transformed_dynamic_frame")
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_frame}, glue_context)


def conversion_process(glue_context, dfc):
    """Process latitude/longitude and date conversions."""
    dynamic_frame = dfc.select(list(dfc.keys())[0])
    df = dynamic_frame.toDF()

    lat_lon_cache = {}

    def get_lat_lon(prefecture, city, address):
        if not all([prefecture, city, address]):
            return None, None

        key = f"{prefecture}{city}{address}"
        if key in lat_lon_cache:
            return lat_lon_cache[key]

        url = f"https://msearch.gsi.go.jp/address-search/AddressSearch?q={prefecture}{city}{address}"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200 and response.json():
                data = response.json()
                lat_lon_cache[key] = (
                    data[0]["geometry"]["coordinates"][1],
                    data[0]["geometry"]["coordinates"][0],
                )
                return lat_lon_cache[key]
        except Exception as e:
            logger.error(f"Geocoding request failed: {e}")
        return None, None

    def get_lat_lon_udf(prefecture, city, address, lat, lon):
        return (lat, lon) if lat is not None and lon is not None else get_lat_lon(
            prefecture, city, address
        )

    schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
        ]
    )

    lat_lon_udf = udf(get_lat_lon_udf, schema)
    df = df.withColumn(
        "lat_lon",
        lat_lon_udf(col("都道府県"), col("市区町村"), col("住所"), col("緯度"), col("経度")),
    )
    df = df.withColumn("緯度", col("lat_lon").getItem("lat")).withColumn(
        "経度", col("lat_lon").getItem("lon")
    ).drop("lat_lon")

    def convert_to_iso8601(date_str):
        if not date_str:
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
            "%Y-%m-%dT%H:%M:%S",
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).isoformat()
            except (ValueError, TypeError):
                continue
        return None

    iso8601_udf = udf(convert_to_iso8601, StringType())
    df = df.withColumn("利用開始日時", iso8601_udf(col("利用開始日時"))).withColumn(
        "利用終了日時", iso8601_udf(col("利用終了日時"))
    )

    transformed_frame = DynamicFrame.fromDF(df, glue_context, "transformed_dynamic_frame")
    return DynamicFrameCollection({"transformed_dynamic_frame": transformed_frame}, glue_context)


def spark_sql_query(glue_context, query, mapping, transformation_ctx):
    """Execute a Spark SQL query with mapped DynamicFrames."""
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = glue_context.spark_session.sql(query)
    return DynamicFrame.fromDF(result, glue_context, transformation_ctx)


def output_list_ap_table(glue_context, dfc):
    """Output AP table data to S3 or local storage."""
    df = dfc.select(list(dfc.keys())[0]).toDF().coalesce(1)
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")

    if IS_LOCAL:
        file_path = os.path.join(OUTPUT_DIR, "list_ap_table")
        file_name = f"listap{args.YYYYMM}.csv"
        os.makedirs(file_path, exist_ok=True)
        full_path = os.path.join(file_path, file_name)
        df.toPandas().to_csv(full_path, index=False, encoding="utf-8")
        logger.info(f"Saved file: {full_path}")
        s3_path = f"s3://{S3_BUCKET}/output/list_ap_output/list_ap_table/{file_name}"
        df.coalesce(1).write.mode("overwrite").csv(s3_path, header=True)
        logger.info(f"Saved to S3: {s3_path}")
    else:
        bucket_name = BUCKET_NAME.OPEN
        file_path = "list_ap/list_ap_table/"
        file_name = f"list_ap_{args.YYYYMM}.csv"
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        temp_path = f"./{file_name}"
        df.toPandas().to_csv(temp_path, index=False, encoding="utf-8")
        s3.upload_file(temp_path, bucket_name, file_path + file_name)
        os.remove(temp_path)

    return DynamicFrameCollection({"output": dynamic_frame}, glue_context)


def output_list_ap_backup(glue_context, dfc):
    """Output AP backup data to S3 or local storage."""
    df = dfc.select(list(dfc.keys())[0]).toDF().coalesce(1)
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")

    if IS_LOCAL:
        file_path = os.path.join(OUTPUT_DIR, "list_ap_backup")
        file_name = f"backup_listap{args.YYYYMM}.csv"
        os.makedirs(file_path, exist_ok=True)
        full_path = os.path.join(file_path, file_name)
        df.toPandas().to_csv(full_path, index=False, encoding="utf-8")
        logger.info(f"Saved file: {full_path}")
        s3_path = f"s3://{S3_BUCKET}/output/list_ap_output/list_ap_backup/{file_name}"
        df.coalesce(1).write.mode("overwrite").csv(s3_path, header=True)
        logger.info(f"Saved to S3: {s3_path}")
    else:
        bucket_name = BUCKET_NAME.OPEN
        file_path = "list_ap/list_ap_backup/"
        file_name = f"backup_list_ap_{args.YYYYMM}.csv"
        s3 = boto3.client("s3")
        temp_path = f"./{file_name}"
        df.toPandas().to_csv(temp_path, index=False, encoding="utf-8")
        s3.upload_file(temp_path, bucket_name, file_path + file_name)
        os.remove(temp_path)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
        if "Contents" in response:
            for obj in response["Contents"]:
                if file_name not in obj["Key"]:
                    s3.delete_object(Bucket=bucket_name, Key=obj["Key"])

    return DynamicFrameCollection({"output": dynamic_frame}, glue_context)


def output_common_ap(glue_context, dfc):
    """Output common AP data to S3 or local storage."""
    df = dfc.select(list(dfc.keys())[0]).toDF().withColumn("yearmonth", col("年月"))
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")

    if IS_LOCAL:
        output_dir = os.path.join(OUTPUT_DIR, "common_ap")
        os.makedirs(output_dir, exist_ok=True)
        for file in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        glue_context.write_dynamic_frame_from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": f"s3://{S3_BUCKET}/output/list_ap_output/common_ap",
                "partitionKeys": ["yearmonth"],
            },
            format_options={"compression": "uncompressed"},
        )
    else:
        bucket_name = BUCKET_NAME.MIDDLE
        base_path = "common/common_ap/"
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=base_path)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format="parquet",
            connection_options={
                "path": f"s3://{bucket_name}/{base_path}",
                "partitionKeys": ["yearmonth"],
            },
            format_options={"compression": "uncompressed"},
        )

    return DynamicFrameCollection({"output": dynamic_frame}, glue_context)


def main():
    """Main function to execute the Glue ETL job."""
    global args
    args = get_resolved_options(sys.argv, ["JOB_NAME", "YYYYMM"])

    if IS_LOCAL:
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Input directory: {INPUT_DIR}")
        logger.info(f"Output directory: {OUTPUT_DIR}")
        logger.info(f"S3 bucket: {S3_BUCKET}")
        logger.info(f"S3 endpoint: {S3_ENDPOINT}")

        conf = (
            SparkSession.builder.appName("list_ap_job")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        spark = conf
        sc = spark.sparkContext
        glue_context = GlueContext(sc)
        job = Job(glue_context)
        job.init(args.JOB_NAME, args)

        try:
            s3_client.list_buckets()
            logger.info("S3/MinIO connection test successful")
        except Exception as e:
            logger.error(f"S3/MinIO connection test failed: {str(e)}")
            sys.exit(1)

        logger.info("Checking input files on S3/MinIO...")
        input_files = {}
        try:
            input_ap_key = "input/csv/input_ap.csv"
            s3_client.head_object(Bucket=S3_BUCKET, Key=input_ap_key)
            input_files["input_ap"] = f"s3://{S3_BUCKET}/{input_ap_key}"
            master_status_key = "master/status/master_status.csv"
            s3_client.head_object(Bucket=S3_BUCKET, Key=master_status_key)
            input_files["master_status"] = f"s3://{S3_BUCKET}/{master_status_key}"
            logger.info("Required files found:")
            for table, path in input_files.items():
                logger.info(f"  - {table}: {path}")
        except Exception as e:
            logger.error(f"Failed to verify files on S3/MinIO: {str(e)}")
            logger.error("Run crawler to upload files to S3/MinIO.")
            sys.exit(1)
    else:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(args.JOB_NAME, args)

    logger.info(
        f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, "
        f"JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"Start Glue Job\"}}"
    )

    try:
        ap_node = glue_context.create_dynamic_frame_from_catalog(
            database="input", table_name="input_ap", transformation_ctx="ap_node"
        )

        status_master_node = glue_context.create_dynamic_frame_from_options(
            format_options={
                "quoteChar": "\"",
                "withHeader": True,
                "separator": ",",
                "optimizePerformance": False,
            },
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [
                    f"s3://{S3_BUCKET if IS_LOCAL else BUCKET_NAME.MASTER}/status/master_status.csv"
                ],
                "recurse": True,
            },
            transformation_ctx="status_master_node",
        )

        process_month_node = process_month(
            glue_context, DynamicFrameCollection({"ap_node": ap_node}, glue_context)
        )
        select_node1 = SelectFromCollection.apply(
            dfc=process_month_node,
            key=list(process_month_node.keys())[0],
            transformation_ctx="SelectFromCollection_node1",
        )

        change_schema_node1 = ApplyMapping.apply(
            frame=select_node1,
            mappings=[
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
                ("yearmonth", "string", "yearmonth", "string"),
            ],
            transformation_ctx="ChangeSchema_node1",
        )

        exception_handling_1_node = exception_handling_1(
            glue_context,
            DynamicFrameCollection(
                {"ChangeSchema_node1": change_schema_node1}, glue_context
            ),
        )
        select_node2 = SelectFromCollection.apply(
            dfc=exception_handling_1_node,
            key=list(exception_handling_1_node.keys())[0],
            transformation_ctx="SelectFromCollection_node2",
        )

        conversion_process_node = conversion_process(
            glue_context,
            DynamicFrameCollection(
                {"SelectFromCollection_node2": select_node2}, glue_context
            ),
        )
        select_node3 = SelectFromCollection.apply(
            dfc=conversion_process_node,
            key=list(conversion_process_node.keys())[0],
            transformation_ctx="SelectFromCollection_node3",
        )

        sql_query = """
        SELECT
            CASE
                WHEN apid RLIKE '([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}' THEN
                    REGEXP_REPLACE(apid, '[-:]', '')
                ELSE
                    apid
            END AS `管理ID`,
            apid AS `APID`,
            `ap名称` AS `AP名称`,
            `設置場所名称`,
            `都道府県`,
            `市区町村`,
            `住所`,
            `補足情報`,
            `緯度`,
            `経度`,
            COALESCE(master_status.`共通ログステータス`, 'その他') AS `ステータス`,
            `利用開始日時`,
            `利用終了日時`,
            `カテゴリ`,
            `保有主体属性`,
            `施設属性`,
            `局属性`,
            yearmonth AS `年月`,
            `事業者`
        FROM ap_status
        LEFT OUTER JOIN master_status
        ON ap_status.`ステータス` = master_status.`各社ステータス`
        """
        translate_ap_id_status_node = spark_sql_query(
            glue_context,
            query=sql_query,
            mapping={
                "ap_status": select_node3,
                "master_status": status_master_node,
            },
            transformation_ctx="Translateap_IDStatusunification_node",
        )

        exception_handling_2_node = exception_handling_2(
            glue_context,
            DynamicFrameCollection(
                {"Translateap_IDStatusunification_node": translate_ap_id_status_node},
                glue_context,
            ),
        )
        select_node4 = SelectFromCollection.apply(
            dfc=exception_handling_2_node,
            key=list(exception_handling_2_node.keys())[0],
            transformation_ctx="SelectFromCollection_node4",
        )

        change_schema_node2 = ApplyMapping.apply(
            frame=select_node4,
            mappings=[
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
                ("事業者", "string", "事業者", "string"),
            ],
            transformation_ctx="ChangeSchema_node2",
        )

        output_list_ap_table(
            glue_context,
            DynamicFrameCollection(
                {"ChangeSchema_node2": change_schema_node2}, glue_context
            ),
        )
        output_common_ap(
            glue_context,
            DynamicFrameCollection(
                {"ChangeSchema_node2": change_schema_node2}, glue_context
            ),
        )
        output_list_ap_backup(
            glue_context,
            DynamicFrameCollection(
                {"ChangeSchema_node2": change_schema_node2}, glue_context
            ),
        )

        logger.info(
            f"{{{datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d %H:%M:%S')}, "
            f"JobID: {args.JOB_RUN_ID}, Level:INFO, Message:\"End Glue Job\"}}"
        )
    except Exception as e:
        logger.error(f"Error during job execution: {str(e)}")
        raise
    finally:
        job.commit()

    if IS_LOCAL:
        logger.info(f"Job completed successfully. Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
