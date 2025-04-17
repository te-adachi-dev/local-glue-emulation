import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Glueジョブ初期化
sc = SparkContext()
glueContext = GlueContext(sc)

# 引数の取得
if len(sys.argv) > 1:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database', 'table'])
else:
    args = {
        'JOB_NAME': 'local_glue_job',
        'database': 'test_db_20250407',
        'table': 'test_table'
    }

# Sparkセッションの初期化
spark = SparkSession.builder \
    .appName(args['JOB_NAME']) \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "/home/glue_user/workspace/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # CSVファイルから直接読み込む
    csv_path = f"/home/glue_user/workspace/data/input/csv/{args['table']}.csv"
    if os.path.exists(csv_path):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print(f"Successfully read data from CSV file: {csv_path}")
    else:
        # Hiveからテーブルを読み込む
        df = spark.sql(f"SELECT * FROM {args['database']}.{args['table']}")
        print(f"Successfully read data from Hive table: {args['database']}.{args['table']}")

except Exception as e:
    print(f"Error reading data: {e}")
    sys.exit(1)

# データの表示
print("Data sample:")
df.show(10)

# データ変換処理
print("Processing data...")
processed_df = df.withColumnRenamed(df.columns[0], "id").cache()

# 結果の表示
print("Processed data:")
processed_df.show(10)

# 結果をParquetとして保存
output_path = "/home/glue_user/workspace/data/output/processed_data"
print(f"Saving results to {output_path}")
processed_df.write.mode("overwrite").parquet(output_path)

print("Job completed successfully!")
job.commit()