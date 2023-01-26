-- FIRST MAP THE CSV DATA THAT IS DOWNLOADED IN HDFS NODE
create external table thanushrir_country_data_csv (
ID string, 
Country string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/inputs/thanushrir_country';



-- RUN A TEST QUERY TO MAKE SURE THE ABOVE WORKED CORRECTLY
select * from thanushrir_country_data_csv limit 10;



-- CREATE AN ORC TABLE FOR THE GREENHOUSE EMISSIONS DATA
-- ("Stored as ORC in the end")
create external table thanushrir_country_data (
ID string, 
Country string
)
stored as orc;



-- COPY THE CSV TABLE TO THE ORC TABLE
insert overwrite table thanushrir_country_data
select * from thanushrir_country_data_csv;


select * from thanushrir_country_data limit 10;