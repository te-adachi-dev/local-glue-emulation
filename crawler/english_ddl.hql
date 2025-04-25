CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_connection (
  `apid` STRING,
  `start_time` STRING, 
  `user_attr` STRING,
  `device_attr` STRING,
  `provider` STRING,
  `yearmonth` INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\"",
  "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_connection'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_ap (
  `apid` STRING,
  `ap_name` STRING,
  `location` STRING,
  `prefecture` STRING,
  `city` STRING,
  `address` STRING,
  `additional_info` STRING,
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `status` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `category` STRING,
  `owner_attr` STRING,
  `facility_attr` STRING,
  `station_attr` STRING,
  `provider` STRING,
  `yearmonth` INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\"",
  "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_ap'
TBLPROPERTIES ('skip.header.line.count'='1');
