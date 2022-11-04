import findspark
findspark.init()

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("clean_data")\
    .getOrCreate()

region = sys.argv[1]

with open(f"Spark_project/Youtube_Trending/Raw_data/{region}/{region}_category_id.json") as file:
    json_file = json.load(file)

category_df = spark.createDataFrame(json_file["items"])
category_df = category_df\
    .withColumn("category_title", f.expr("snippet.title"))\
    .select(f.col("id"), f.col("category_title"))

video_df = spark.read\
    .format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .option("path", f"Spark_project/Youtube_Trending/Raw_data/{region}/*.csv")\
    .load()

video_df = video_df\
    .select("video_id", "title", "channel_title", "category_id", f.col("publish_time").cast("date"), "views", "likes", "dislikes", "comment_count")\
    .withColumn("Region", f.lit(f"{region}"))

join_df = video_df.join(category_df, video_df.category_id == category_df.id, "inner")\
    .drop("id", "category_id")\
    .na.drop()

join_df.write\
    .format("csv")\
    .option("header", "true")\
    .mode("overwrite")\
    .option("path", f"Spark_project/Youtube_Trending/Clean_data/{region}/")\
    .save()


