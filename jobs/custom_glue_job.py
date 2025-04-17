import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, when
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Glueジョブ初期化
sc = SparkContext()
glueContext = GlueContext(sc)

# 引数の取得
if len(sys.argv) > 1:
    args = getResolvedOptions(sys.argv, 
                             ['JOB_NAME', 'database', 'input_table', 'output_table'])
else:
    args = {
        'JOB_NAME': 'custom_glue_job',
        'database': 'test_db_20250407',
        'input_table': 'source_table',
        'output_table': 'target_table'
    }

# Sparkセッションの初期化
spark = SparkSession.builder \
    .appName(args['JOB_NAME']) \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/opt/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # データの読み込み
    source_table = f"{args['database']}.{args['input_table']}"
    print(f"Reading data from {source_table}")
    
    # まずHiveテーブルからの読み込みを試す
    try:
        df = spark.sql(f"SELECT * FROM {source_table}")
        print(f"Successfully read from Hive table: {source_table}")
    except:
        # Hiveテーブルがない場合はCSVから直接読み込む
        csv_path = f"/home/glue_user/workspace/data/input/csv/{args['input_table']}.csv"
        if os.path.exists(csv_path):
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            print(f"Successfully read from CSV file: {csv_path}")
        else:
            raise Exception(f"Neither Hive table {source_table} nor CSV file {csv_path} found")

    # データの確認
    print("Input data sample:")
    df.printSchema()
    df.show(5)
    
    # データ変換処理の実装
    def transform_data(input_df: DataFrame) -> DataFrame:
        """
        データ変換処理を実装
        ここに業務ロジックを記述する
        """
        # 列名を小文字に変換
        lower_cols = [col(c).alias(c.lower()) for c in input_df.columns]
        processed_df = input_df.select(*lower_cols)
        
        # 型変換やその他の変換処理
        for column in processed_df.columns:
            # 数値型の列は欠損値を0に置換
            if processed_df.schema[column].dataType.typeName() in ['integer', 'long', 'double']:
                processed_df = processed_df.withColumn(
                    column, 
                    when(col(column).isNull(), 0).otherwise(col(column))
                )
                
        return processed_df
    
    # 変換処理の実行
    print("Transforming data...")
    processed_df = transform_data(df)
    
    # 結果の確認
    print("Transformed data sample:")
    processed_df.printSchema()
    processed_df.show(5)
    
    # 結果の保存（Parquet形式）
    output_path = f"/home/glue_user/workspace/data/output/{args['output_table']}"
    print(f"Saving results to {output_path}")
    processed_df.write.mode("overwrite").parquet(output_path)
    
    # 結果をHiveテーブルとしても保存
    output_table = f"{args['database']}.{args['output_table']}"
    print(f"Saving results to Hive table: {output_table}")
    processed_df.write.mode("overwrite").saveAsTable(output_table)
    
    print("Job completed successfully!")

except Exception as e:
    print(f"Error in job execution: {e}")
    sys.exit(1)

job.commit()
