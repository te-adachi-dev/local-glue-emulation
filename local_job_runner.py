#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import logging
import importlib.util
from local_spark_session import get_spark_session, JobMock, getResolvedOptions

# ロギングの設定
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('job_runner')

# localjob_runner.py - 必要な変更部分

def run_job(job_path, job_name="local_test_job", job_args=None):
    """
    ローカルでGlueジョブを実行する
    
    Args:
        job_path (str): ジョブスクリプトのパス
        job_name (str): ジョブ名
        job_args (dict): ジョブ引数
    """
    if job_args is None:
        job_args = {}
    
    # 引数の準備
    sys_args = ["script_name"]
    for key, value in job_args.items():
        sys_args.extend([f"--{key}", str(value)])
    
    # スクリプトのパスが存在するか確認
    if not os.path.exists(job_path):
        logger.error(f"Job script not found: {job_path}")
        return False
    
    try:
        # Sparkセッションの初期化
        spark, sc, glue_context = get_spark_session(job_name)
        job = JobMock(glue_context)
        job.init(job_name, job_args)
        
        # 入力データの準備（テーブルを作成）
        if 'table' in job_args:
            table_name = job_args['table']
            csv_path = os.path.join(os.getcwd(), "data", "input", "csv", f"{table_name}.csv")
            if os.path.exists(csv_path):
                try:
                    # テーブルがなければ作成
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
                    df.createOrReplaceTempView(table_name)
                    logger.info(f"Created table {table_name} from {csv_path}")
                except Exception as e:
                    logger.error(f"Error creating table {table_name}: {e}")
        
        # グローバル変数の設定
        globals_dict = {
            "spark": spark,
            "sc": sc,
            "glueContext": glue_context,
            "job": job,
            "args": job_args,
            "getResolvedOptions": getResolvedOptions,
            "sys": sys
        }
        
        # システム引数の一時的な置き換え
        original_sys_argv = sys.argv
        sys.argv = sys_args
        
        # ジョブスクリプトをモジュールとして読み込む
        spec = importlib.util.spec_from_file_location("job_module", job_path)
        job_module = importlib.util.module_from_spec(spec)
        
        # モジュールに必要なグローバル変数を設定
        for key, value in globals_dict.items():
            setattr(job_module, key, value)
        
        # スクリプトを実行
        logger.info(f"Executing job: {job_name} from {job_path}")
        spec.loader.exec_module(job_module)
        
        # システム引数を元に戻す
        sys.argv = original_sys_argv
        
        logger.info(f"Job {job_name} executed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error executing job {job_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # システム引数を元に戻す
        if 'original_sys_argv' in locals():
            sys.argv = original_sys_argv
        
        return False

def main():
    parser = argparse.ArgumentParser(description='Run Glue job locally')
    parser.add_argument('--job_script', type=str, required=True,
                        help='Path to the job script')
    parser.add_argument('--job_name', type=str, default='local_test_job',
                        help='Job name')
    parser.add_argument('--database', type=str, default='test_db_20250415',
                        help='Database name')
    parser.add_argument('--table', type=str, default='test_table',
                        help='Table name')
    
    args = parser.parse_args()
    
    # ジョブ引数の準備
    job_args = {
        "JOB_NAME": args.job_name,
        "database": args.database,
        "table": args.table
    }
    
    # ジョブの実行
    success = run_job(args.job_script, args.job_name, job_args)
    
    # 終了コード設定
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()