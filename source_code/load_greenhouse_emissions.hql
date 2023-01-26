-- FIRST MAP THE CSV DATA THAT IS DOWNLOADED IN HDFS NODE
create external table thanushrir_greenhouse_emissions_csv ( 
Country string, 
Code string, 
Year smallint, 
Emissions bigint
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/inputs/greenhouse_emissions/';



-- RUN A TEST QUERY TO MAKE SURE THE ABOVE WORKED CORRECTLY
select * from thanushrir_greenhouse_emissions_csv limit 10;
select * from thanushrir_greenhouse_emissions_csv limit 10 offset 200;


-- CREATE AN ORC TABLE FOR THE GREENHOUSE EMISSIONS DATA
-- ("Stored as ORC in the end")
create external table thanushrir_greenhouse_emissions (
Country string, 
Year smallint, 
Emissions bigint
)
stored as orc;



-- COPY THE CSV TABLE TO THE ORC TABLE
insert overwrite table thanushrir_greenhouse_emissions 
select Country, Year, Emissions from thanushrir_greenhouse_emissions_csv;


select * from thanushrir_greenhouse_emissions limit 10;
select * from thanushrir_greenhouse_emissions limit 10 offset 200;