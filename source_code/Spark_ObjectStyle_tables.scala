// Spark Object Style reimplementation of the batch layer
// spark-shell --jars /home/hadoop/thanushrir/src/target/thanushrir-1.0-SNAPSHOT.jar

// Getting all the base tables ready in spark (using the names of the hive tables)
val thanushrir_project_weathersummary = spark.table("thanushrir_project_weathersummary")

thanushrir_project_weathersummary.take(5)


val thanushrir_greenhouse_emissions = spark.table("thanushrir_greenhouse_emissions")
thanushrir_greenhouse_emissions.take(5)

val thanushrir_country_data = spark.table("thanushrir_country_data")
thanushrir_country_data.take(5)


// Creating tables with the existing tables
val thanushrir_project_greenhouse_oo = thanushrir_country_data.join(thanushrir_greenhouse_emissions, thanushrir_country_data("country") <=> thanushrir_greenhouse_emissions("country")).select(thanushrir_country_data("ID"),thanushrir_greenhouse_emissions("country"),thanushrir_greenhouse_emissions("year"),thanushrir_greenhouse_emissions("emissions"))

thanushrir_project_greenhouse_oo.show(2)



val thanushrir_project_group_weatherData_oo = thanushrir_project_weathersummary.groupBy(thanushrir_project_weathersummary("countrycode"),thanushrir_project_weathersummary("year")).agg(avg(thanushrir_project_weathersummary("meantemperature")).as("avg_temp"))

thanushrir_project_group_weatherData_oo.show(2)



val thanushrir_project_weather_greenhouse_oo = thanushrir_project_group_weatherData_oo.join(thanushrir_project_greenhouse_oo, thanushrir_project_group_weatherData_oo("countrycode") <=> thanushrir_project_greenhouse_oo("ID") && thanushrir_project_group_weatherData_oo("year") <=> thanushrir_project_greenhouse_oo("year")).select(thanushrir_project_group_weatherData_oo("countrycode"),thanushrir_project_greenhouse_oo("country"),thanushrir_project_group_weatherData_oo("year"),thanushrir_project_greenhouse_oo("emissions"),thanushrir_project_group_weatherData_oo("avg_temp"))

thanushrir_project_weather_greenhouse_oo.show(2)


// Save to Hive because bulk loading HBase from Spark is awkward
import org.apache.spark.sql.SaveMode
thanushrir_project_weather_greenhouse_oo.write.mode(SaveMode.Overwrite).saveAsTable("thanushrir_project_weather_greenhouse_oo")