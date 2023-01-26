create external table thanushrir_project_greenhouse ( 
ID string, 
Country string,
Year smallint, 
Emissions bigint
)
stored as orc;



insert overwrite table thanushrir_project_greenhouse 
select cd.ID, ge.*
from thanushrir_country_data cd
join thanushrir_greenhouse_emissions ge 
on cd.country = ge.country;



select * from thanushrir_project_greenhouse limit 10;