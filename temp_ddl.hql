CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.test_table (
  `id` INT,
  `name` STRING,
  `age` INT,
  `department` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/home/glue_user/workspace/data/warehouse/test_table'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.employee_data (
  `id` INT,
  `employee_name` STRING,
  `department` STRING,
  `salary` INT,
  `hire_date` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/home/glue_user/workspace/data/warehouse/employee_data'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.wifi_access_logs (
  `mac_address` STRING,
  `connection_start` STRING,
  `connection_end` STRING,
  `access_point_id` STRING,
  `signal_strength` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/home/glue_user/workspace/data/warehouse/wifi_access_logs'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.wifi_access_log (
  `mac_address` STRING,
  `connection_start` STRING,
  `connection_end` STRING,
  `access_point_id` STRING,
  `signal_strength` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/home/glue_user/workspace/data/warehouse/wifi_access_log'
TBLPROPERTIES ('skip.header.line.count'='1');

