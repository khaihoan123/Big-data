--Drops existing database
DROP DATABASE IF EXISTS crime CASCADE;

--Creates new database 
CREATE DATABASE IF NOT EXISTS crime;

--Use database
USE crime;

--Creates factcrime table
CREATE TABLE factcrime
(id STRING,
time_id STRING,
crime_type_id INT,
lSOA_code STRING,
police_station_id INT,
outcome_id INT,
imd_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim time table
CREATE TABLE dimtime
(id STRING,
year INT,
month INT,
season STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim crime type table
CREATE TABLE dimcrimetype
(id INT,
name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim lsoa table
CREATE TABLE dimlsoa
(lSOA_code STRING,
lsoa_name STRING,
ward_code STRING,
ward_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim police station table
CREATE TABLE dimpolicestation
(id INT,
name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim outcome table
CREATE TABLE dimoutcome
(
id INT,
name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Creates dim IMD table
CREATE TABLE dimimd
(
id STRING,
score DOUBLE,
lSOA_code STRING,
year INT,
applicable_from_year INT,
applicable_to_year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

--Loads data from local data file
LOAD DATA LOCAL INPATH '/home/maria_dev/input/fact_table.csv' INTO TABLE factcrime;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/time.csv' INTO TABLE dimtime;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/crime_type.csv' INTO TABLE dimcrimetype;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/imd.csv' INTO TABLE dimimd;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/LSOA.csv' INTO TABLE dimlsoa;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/outcomes.csv' INTO TABLE dimoutcome;
LOAD DATA LOCAL INPATH '/home/maria_dev/input/police_stations.csv' INTO TABLE dimpolicestation;


--Get the number of occurences for each ratings
-- SELECT rating, count(rating) as total_occurences
--FROM Ratings
--GROUP BY rating
--ORDER BY total_occurences DESC;