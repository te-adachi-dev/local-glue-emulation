import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from awsglue.context import GlueContext
from awsglue.job import Job

# Glueジョブ初期化
sc = SparkContext()
glueContext = GlueContext(sc)

# Sparkセッションの初期化 - 設定を変更
spark = SparkSession.builder \
    .appName("EmployeeDataProcessing") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/opt/hive/warehouse") \
    .config("hive.metastore.client.factory.class", "") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "") \
    .enableHiveSupport() \
    .getOrCreate()

# データベースとテーブル名の指定
database = "test_db_20250407"
table = "employee_data"

try:
    # Hiveテーブルからデータ読み込み
    print(f"Reading data from {database}.{table}")
    df = spark.sql(f"SELECT * FROM {database}.{table}")
    print("Data read successfully:")
    df.show()

    # データ変換: 給与に10%ボーナスを追加
    print("Processing data - Adding 10% bonus to salaries")
    processed_df = df.withColumn("salary_with_bonus",
                                col("salary") * 1.1)

    # 結果の表示
    print("Processed data:")
    processed_df.show()

    # 結果をParquetとして保存
    output_path = "/home/glue_user/workspace/data/output/processed_employee_data"
    print(f"Saving results to {output_path}")
    processed_df.write.mode("overwrite").parquet(output_path)

    # 処理済みデータをHiveテーブルとして保存
    output_table = f"{table}_with_bonus"
    print(f"Saving data to Hive table: {database}.{output_table}")
    processed_df.write.mode("overwrite").saveAsTable(f"{database}.{output_table}")

    print("Job completed successfully!")

except Exception as e:
    print(f"Error in job execution: {e}")