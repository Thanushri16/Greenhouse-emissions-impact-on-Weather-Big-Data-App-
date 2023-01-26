create table thanushrir_project_group_weatherData
(
countrycode string,
year smallint,
avg_temp float
) stored as orc;

insert overwrite table thanushrir_project_group_weatherData
select countrycode, year, avg(meantemperature) as avg_temp 
from thanushrir_project_weathersummary
group by countrycode, year 
order by countrycode, year;


