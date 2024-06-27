#!/usr/bin/python

#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext


file_path = "file:///home/talentum/test-jupyter/practice/constitution.txt"

print("=====================Output Here======================")

print(sc.textFile(file_path)\
.flatMap(lambda x: x.split())\
.filter(lambda x: x.lower())\
.map(lambda w: (w, 1))\
.reduceByKey(lambda x, y: x + y)\
.map(lambda x: (x[1], x[0]))\
.sortByKey(ascending=False).take(10))

