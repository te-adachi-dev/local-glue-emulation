CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.test_table (
  id INT,
  name STRING,
  age INT,
  department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/test_db_20250407.db/test_table'
TBLPROPERTIES ('skip.header.line.count'='1');
