create external table thanushrir_project_hbase_HiveStyle (
  id_year string, 
  country string,
  emissions bigint,
  avg_temp float
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,values:country,values:emissions,values:avg_temp')
TBLPROPERTIES ('hbase.table.name' = 'thanushrir_project_hbase_HiveStyle');


insert overwrite table thanushrir_project_hbase_HiveStyle
select concat(id, year),
country, emissions, 
avg_temp from thanushrir_project_weather_greenhouse;
