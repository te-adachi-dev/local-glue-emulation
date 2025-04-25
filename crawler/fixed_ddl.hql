CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_connection (
  apid STRING,
  use_start_datetime STRING,
  user_attr STRING,
  device_attr STRING,
  provider STRING,
  yearmonth INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_connection'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_ap (
  apid STRING,
  ap_name STRING,
  location_name STRING,
  prefecture STRING,
  city STRING,
  address STRING,
  additional_info STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  status STRING,
  use_start_datetime STRING,
  use_end_datetime STRING,
  category STRING,
  owner_attr STRING,
  facility_attr STRING,
  station_attr STRING,
  provider STRING,
  yearmonth INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_ap'
TBLPROPERTIES ('skip.header.line.count'='1');
