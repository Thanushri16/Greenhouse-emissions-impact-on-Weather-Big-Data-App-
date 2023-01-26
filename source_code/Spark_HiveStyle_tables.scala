// Spark Hive Style reimplementation of the batch layer
// spark-shell --jars /home/hadoop/thanushrir/src/target/thanushrir-1.0-SNAPSHOT.jar

// Getting all the base tables ready in spark (using the names of the hive tables)
val thanushrir_project_weathersummary = spark.table("thanushrir_project_weathersummary")

thanushrir_project_weathersummary.take(5)


val thanushrir_greenhouse_emissions = spark.table("thanushrir_greenhouse_emissions")
thanushrir_greenhouse_emissions.take(5)

val thanushrir_country_data = spark.table("thanushrir_country_data")
thanushrir_country_data.take(5)

// Creating tables with the existing tables
val thanushrir_project_greenhouse = spark.sql("""select cd.ID, ge.*
from thanushrir_country_data cd
join thanushrir_greenhouse_emissions ge 
on cd.country = ge.country""")

// Make a spark dataframe visible to spark.sql
thanushrir_project_greenhouse.createOrReplaceTempView("thanushrir_project_greenhouse")
thanushrir_project_greenhouse.show(2)


val thanushrir_project_group_weatherData = spark.sql("""select countrycode, year, avg(meantemperature) as avg_temp 
from thanushrir_project_weathersummary
group by countrycode, year 
order by countrycode, year""")

// Make a spark dataframe visible to spark.sql
thanushrir_project_group_weatherData.createOrReplaceTempView("thanushrir_project_group_weatherData")
thanushrir_project_group_weatherData.show(2)



val thanushrir_project_weather_greenhouse_h = spark.sql("""select we.countrycode, ge.country, we.year, ge.emissions, we.avg_temp
from thanushrir_project_group_weatherdata we
join thanushrir_project_greenhouse ge 
on we.countrycode = ge.ID 
and we.year = ge.year""")

// Make a spark dataframe visible to spark.sql
thanushrir_project_weather_greenhouse_h.createOrReplaceTempView("thanushrir_project_weather_greenhouse_h")
thanushrir_project_weather_greenhouse_h.show(2)


// Save to Hive because bulk loading HBase from Spark is awkward
import org.apache.spark.sql.SaveMode
thanushrir_project_weather_greenhouse_h.write.mode(SaveMode.Overwrite).saveAsTable("thanushrir_project_weather_greenhouse_h")
