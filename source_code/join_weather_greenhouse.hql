create external table thanushrir_project_weather_greenhouse ( 
ID string, 
Country string,
Year smallint, 
Emissions bigint,
Avg_temp float
)
stored as orc;



insert overwrite table thanushrir_project_weather_greenhouse 
select we.countrycode, ge.country, we.year, ge.emissions, we.avg_temp
from thanushrir_project_group_weatherdata we
join thanushrir_project_greenhouse ge 
on we.countrycode = ge.ID 
and we.year = ge.year;



select * from thanushrir_project_weather_greenhouse limit 10;