create external table thanushrir_project_spark_hivestyle (
  id_year string, 
  country string,
  emissions bigint,
  avg_temp float
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,values:country,values:emissions,values:avg_temp')
TBLPROPERTIES ('hbase.table.name' = 'thanushrir_project_spark_hivestyle');


insert overwrite table thanushrir_project_spark_hivestyle
select concat(countrycode, year),
country, emissions, 
avg_temp from thanushrir_project_weather_greenhouse_h;