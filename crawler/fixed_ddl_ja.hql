CREATE DATABASE IF NOT EXISTS test_db_20250407;

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_connection (
  `apid` STRING,
  `利用開始日時` STRING,
  `利用者属性` STRING,
  `端末属性` STRING,
  `事業者` STRING,
  `yearmonth` INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_connection'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.input_ap (
  `apid` STRING,
  `ap名称` STRING,
  `設置場所名称` STRING,
  `都道府県` STRING,
  `市区町村` STRING,
  `住所` STRING,
  `補足情報` STRING,
  `緯度` DOUBLE,
  `経度` DOUBLE,
  `ステータス` STRING,
  `利用開始日時` STRING,
  `利用終了日時` STRING,
  `カテゴリ` STRING,
  `保有主体属性` STRING,
  `施設属性` STRING,
  `局属性` STRING,
  `事業者` STRING,
  `yearmonth` INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/warehouse/input_ap'
TBLPROPERTIES ('skip.header.line.count'='1');
