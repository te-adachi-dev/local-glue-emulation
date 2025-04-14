import argparse
import pandas as pd
from sqlalchemy import create_engine

def execute_sql(query, metastore_uri="localhost:10000"):
    """
    Hiveに対してSQLクエリを実行し、結果を表示する
    """
    conn_string = f"hive://{metastore_uri}"
    engine = create_engine(conn_string)
    
    try:
        # クエリ実行
        result = pd.read_sql(query, engine)
        print(f"Query executed successfully. Result has {len(result)} rows.")
        print(result.head(20))  # 先頭20行を表示
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute SQL query against Hive Metastore')
    parser.add_argument('--query', type=str, required=True, help='SQL query to execute')
    parser.add_argument('--output', type=str, help='Output file path (CSV format)')
    
    args = parser.parse_args()
    
    result = execute_sql(args.query)
    
    if result is not None and args.output:
        result.to_csv(args.output, index=False)
        print(f"Results saved to {args.output}")
