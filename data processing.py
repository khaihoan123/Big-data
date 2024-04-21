from pyspark.sql import SparkSession
from glob import glob
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce
from itertools import chain

import pandas as pd
import pyspark

street_files = glob('C:/Users/LabStudent-55-706949/Downloads/policedata/policedata/*')
month_season = {
    3: 'Spring',
    4: 'Spring',
    5: 'Spring',
    6: 'Summer',
    7: 'Summer',
    8: 'Summer',
    9: 'Autumn',
    10: 'Autumn',
    11: 'Autumn',
    12: 'Winter',
    1: 'Winter',
    2: 'Winter',
    }

try:
    spark = SparkSession.builder.appName("FileTransformation").config("spark.driver.memory", "20g").getOrCreate()

    df = spark.read.csv(street_files, header=True).sort('Month', ascending=False )
    df = df.select('Crime ID', 'Month', 'Crime type', 'Reported by','LSOA code', 'LSOA name', 'Last outcome category')
    imd_2010 = pd.read_excel(io = 'C:/Users/LabStudent-55-706949/Downloads/AdjustedIMD2010scoresfor2011LSOAs.xlsx',  sheet_name = 'England', skiprows=[0,1])
    imd_2010 = spark.createDataFrame(imd_2010)
    
    imd_2015 = pd.read_excel(io = 'C:/Users/LabStudent-55-706949/Downloads/ID2015.xls',  sheet_name = 'IMD 2015')
    imd_2015 = spark.createDataFrame(imd_2015)

    imd_2019 = pd.read_excel(io = 'C:/Users/LabStudent-55-706949/Downloads/ID2019.xlsx',  sheet_name = 'IMD 2019')
    imd_2019 = spark.createDataFrame(imd_2019)

    print('number of rows initially: ', df.count())
    
    
    df = df.na.drop(subset=['LSOA code', 'Crime ID'])
    print('number of rows after drop null value on LSOA and Crime ID: ', df.count())
    if df.count() > df.dropDuplicates(['Crime ID']).count():
        df = df.dropDuplicates(['Crime ID'])
        print('number of rows after drop duplicate rows by Crime ID: ', df.count())
    
    # cols_with_nulls = [x for x in df.columns if df.filter(F.col(x).isNull()).count() > 0]
    # print('Columns still have null value: ', str(cols_with_nulls))

    imd_2010 = imd_2010.withColumn('year', lit('2010')) \
        .withColumn('id', F.format_string("%s-%s", "LSOA11CD", lit('2010'))) \
        .withColumn('applicable_from_year', lit('2013')) \
        .withColumn('applicable_to_year', lit('2014')) \
        .withColumnRenamed('LSOA11CD', 'lsoa_code') \
        .withColumnRenamed('IMD 2010 adjusted', 'score')
    imd_2010 = imd_2010.select('id', 'score', 'lsoa_code', 'year', 'applicable_from_year', 'applicable_to_year')

    imd_2015 = imd_2015.withColumn('year', lit('2015')) \
        .withColumn('id', F.format_string("%s-%s", "LSOA code (2011)", lit('2015'))) \
        .withColumn('applicable_from_year', lit('2015')) \
        .withColumn('applicable_to_year', lit('2018')) \
        .withColumnRenamed('LSOA code (2011)', 'lsoa_code') \
        .withColumnRenamed('IMD Score', 'score')
    imd_2015 = imd_2015.select('id', 'score', 'lsoa_code', 'year', 'applicable_from_year', 'applicable_to_year')

    imd_2019 = imd_2019.withColumn('year', lit('2019')) \
        .withColumn('id', F.format_string("%s-%s", "LSOA code (2011)", lit('2019'))) \
        .withColumn('applicable_from_year', lit('2019')) \
        .withColumn('applicable_to_year', lit('2023')) \
        .withColumnRenamed('LSOA code (2011)', 'lsoa_code') \
        .withColumnRenamed('Index of Multiple Deprivation (IMD) Score', 'score')
    imd_2019 = imd_2019.select('id', 'score', 'lsoa_code', 'year', 'applicable_from_year', 'applicable_to_year')
    imd = reduce(pyspark.sql.dataframe.DataFrame.unionByName, [imd_2010, imd_2015, imd_2019]).na.drop()
    
    w = Window.partitionBy(lit(1)).orderBy(lit(1))

    police_stations = df.select('Reported by').distinct().withColumn("id", row_number().over(w))
    police_stations = police_stations.select("id", 'Reported by').withColumnRenamed("Reported by", "name")

    crime_type = df.select('Crime type').distinct().withColumn("id", row_number().over(w))
    crime_type = crime_type.select("id", 'Crime type').withColumnRenamed("Crime type", "name")

    LSOA = df.select('LSOA code', 'LSOA name').distinct()

    outcomes = df.select('Last outcome category').distinct().withColumn("id", row_number().over(w))
    outcomes = outcomes.select("id", 'Last outcome category').withColumnRenamed("Last outcome category", "name")

    mapping_expr = create_map([lit(x) for x in chain(*month_season.items())])
    time = df.withColumn('id', col("Month")).withColumn("year", year(col("Month"))).withColumn("month", month(col("Month")))
    time = time.select('id', 'year', 'month').withColumn("season", mapping_expr[col("month")]).distinct().sort('id', ascending=False )

    print('==============')
    df = df \
        .join(police_stations, on=(df['Reported by']==police_stations.name), how='left') \
        .join(crime_type, on=(df['Crime type']==crime_type.name), how='left') \
        .join(outcomes, on=(df['Last outcome category']==outcomes.name), how='left') \
        .join(imd, on=[df['LSOA code']==imd.lsoa_code, imd.applicable_from_year <= year(df["Month"]), year(df["Month"]) <= imd.applicable_to_year ], how='left') \
            .withColumn("police_station_id", police_stations.id) \
            .withColumn("crime_type_id", crime_type.id) \
            .withColumn("outcome_id", outcomes.id) \
            .withColumn('imd_id', imd.id)
    df = df.select('Crime ID', 'Month', 'crime_type_id', 'LSOA code', 'police_station_id', 'outcome_id', 'imd_id')
    print('aaaaaaaaaa')
    df.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/fact_table.csv', index=False)
    police_stations.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/police_stations.csv', index=False)
    crime_type.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/crime_type.csv', index=False)
    outcomes.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/outcomes.csv', index=False)
    LSOA.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/LSOA.csv', index=False)
    time.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/time.csv', index=False)
    imd.toPandas().to_csv('C:/Users/LabStudent-55-706949/Downloads/input/imd.csv', index=False)
    print('--end--')



except Exception as e:
    print("An error occurred:", str(e))
