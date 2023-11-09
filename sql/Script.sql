create table t1 as from read_parquet('D:\DuckDB\stock\trades\20230504.parquet');

SELECT * from t1 limit 10;
SELECT * from t1 where extract('hour' from datetime) <= 9 and extract('minute' from datetime) <= 20 ;
SELECT * from t1 where datetime::TIME <= TIME '09:20:00';
