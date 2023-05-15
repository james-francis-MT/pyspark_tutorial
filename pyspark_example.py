from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

flightData2015 = spark.read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("./flight-data/csv/2015-summary.csv")

spark.conf.set("spark.sql.shuffle.partitions", "5")

flightData2015.createOrReplaceTempView("flight_data_2015")

# SQL way to select top 5 travelled to countries
maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)

maxSql.show()
maxSql.printSchema()

# DataFrame way to do the same thing:
from pyspark.sql.functions import desc

flightData2015\
    .groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total"))\
    .limit(5)\
    .show()