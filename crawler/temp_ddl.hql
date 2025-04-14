CREATE DATABASE IF NOT EXISTS test_db_20250407;

                    CREATE EXTERNAL TABLE IF NOT EXISTS test_db_20250407.test_table (
                      `id` INT,
  `name` STRING,
  `value` DOUBLE
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '/home/ad01/glue_mock/data/input/csv/test_table.csv'
                    TBLPROPERTIES ('skip.header.line.count'='1');
                    
