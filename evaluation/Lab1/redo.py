#!/usr/bin/python/

#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext

file_path = "file:///home/talentum/test-jupyter/evaluation/Lab1/hortonworks.txt"

fileRDD = sc.textFile(file_path)

splitRDD = fileRDD.map(lambda line: line.split(','))

mapRDD = splitRDD.flatMap(lambda line: ((word, line[0]) for word in line[1:]))

finalRDD = mapRDD.reduceByKey(lambda url1, url2: url1 + ',' + url2)

for key, value in finalRDD.collect():
    print(key, value)
