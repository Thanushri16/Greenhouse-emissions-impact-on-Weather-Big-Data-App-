add jar /home/hadoop/thanushrir/src/target/thanushrir-1.0-SNAPSHOT.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS thanushrir_project_weathersummary
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES (
      'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
      'serialization.format' =  'org.apache.thrift.protocol.TBinaryProtocol')
  STORED AS SEQUENCEFILE 
  LOCATION '/inputs/thanushrir_weatherData';

-- Check if our table is created
show tables;

-- Test our table
select * from thanushrir_project_weathersummary limit 5;