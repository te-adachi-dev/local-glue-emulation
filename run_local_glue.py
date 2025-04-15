#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import logging
import subprocess

# Windows環境変数設定
if sys.platform.startswith('win'):
    # Hadoop環境変数の設定
    os.environ["HADOOP_HOME"] = "C:/tools/hadoop-2.7.2"
    os.environ["HADOOP_USER_NAME"] = "hive"
    
    # PATHにHadoopのbinディレクトリを追加
    hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    if hadoop_bin not in os.environ["PATH"]:
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]

# ロギングの設定
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('run_local_glue')

def run_setup():
    """環境のセットアップを実行"""
    logger.info("環境セットアップを実行中...")
    try:
        subprocess.run([sys.executable, "setup_local_env.py"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"環境セットアップ中にエラーが発生しました: {e}")
        return False

def run_crawler(input_path, database):
    """クローラーを実行"""
    logger.info("クローラーを実行中...")
    try:
        subprocess.run([
            sys.executable, "local_crawler.py",
            "--input_path", input_path,
            "--database", database
        ], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"クローラー実行中にエラーが発生しました: {e}")
        return False

def run_job(job_script, job_name, database, table):
    """ジョブを実行"""
    logger.info("ジョブを実行中...")
    try:
        subprocess.run([
            sys.executable, "local_job_runner.py",
            "--job_script", job_script,
            "--job_name", job_name,
            "--database", database,
            "--table", table
        ], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ジョブ実行中にエラーが発生しました: {e}")
        return False

def run_sql(query, output=None):
    """SQLクエリを実行"""
    logger.info("SQLクエリを実行中...")
    cmd = [sys.executable, "local_sql_executor.py", "--query", query]
    
    if output:
        cmd.extend(["--output", output])
    
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"SQLクエリ実行中にエラーが発生しました: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='ローカルGlue環境の操作')
    subparsers = parser.add_subparsers(dest='command', help='実行するコマンド')
    
    # setupコマンド
    setup_parser = subparsers.add_parser('setup', help='環境のセットアップ')
    
    # crawlerコマンド
    crawler_parser = subparsers.add_parser('crawler', help='クローラーの実行')
    crawler_parser.add_argument('--input_path', type=str, default='./data/input',
                                help='入力データのディレクトリパス')
    crawler_parser.add_argument('--database', type=str, default='test_db_20250415',
                                help='使用するデータベース名')
    
    # jobコマンド
    job_parser = subparsers.add_parser('job', help='ジョブの実行')
    job_parser.add_argument('--job_script', type=str, required=True,
                            help='ジョブスクリプトのパス')
    job_parser.add_argument('--job_name', type=str, default='local_test_job',
                            help='ジョブ名')
    job_parser.add_argument('--database', type=str, default='test_db_20250415',
                            help='使用するデータベース名')
    job_parser.add_argument('--table', type=str, default='test_table',
                            help='使用するテーブル名')
    
    # sqlコマンド
    sql_parser = subparsers.add_parser('sql', help='SQLクエリの実行')
    sql_parser.add_argument('--query', type=str, required=True,
                            help='実行するSQLクエリ')
    sql_parser.add_argument('--output', type=str,
                            help='結果を保存するファイルパス')
    
    args = parser.parse_args()
    
    if args.command == 'setup':
        run_setup()
    elif args.command == 'crawler':
        run_crawler(args.input_path, args.database)
    elif args.command == 'job':
        run_job(args.job_script, args.job_name, args.database, args.table)
    elif args.command == 'sql':
        run_sql(args.query, args.output)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()