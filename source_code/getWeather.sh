#!/bin/bash
mkdir weatherData_thanushrir
cd weatherData_thanushrir
year=1970
while [ $year -le 2020 ]
do
    wget ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/$year.csv.gz
    (( year++ ))
done
