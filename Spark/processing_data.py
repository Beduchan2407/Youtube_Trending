from distutils.spawn import spawn
import findspark
findspark.init()

import os
from datetime import date, datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("clean_data")\
    .getOrCreate()

schema = StructType([
    StructField("video_id", StringType()),
    StructField("title", StringType()),
    StructField("channel_title", StringType()),
    StructField("publish_time", DateType()),
    StructField("views", IntegerType()),
    StructField("likes", IntegerType()),
    StructField("dislikes", IntegerType()),
    StructField("comment_count", IntegerType()),
    StructField("region", StringType()),
    StructField("category_title", StringType()),
])

# Read all clean data
full_data_df = spark.read\
    .format("csv")\
    .schema(schema)\
    .option("path", "Spark_project/Youtube_Trending/Clean_data/*/*.csv")\
    .option("header", "true")\
    .load()

# create category_dim table
category_dim_df = full_data_df.select("category_title").distinct()
category_dim_df = category_dim_df.withColumn("category_key", f.row_number().over(Window.orderBy("category_title")))

# create channel_dim table
channel_dim_df = full_data_df.select("channel_title").distinct()
channel_dim_df = channel_dim_df.withColumn("channel_key", f.row_number().over(Window.orderBy("channel_title")))

# create region_dim table
region_dim_df = full_data_df.select("region").distinct()
region_dim_df = region_dim_df.withColumn("region_key", f.row_number().over(Window.orderBy("region")))

# create video_dim table
video_dim_df = full_data_df.select("video_id", "title").distinct()
video_dim_df = video_dim_df.withColumn("video_key", f.row_number().over(Window.orderBy("video_id", "title")))

# create date_dim table
date_dim_df = full_data_df.select("publish_time").distinct()
date_dim_df = date_dim_df\
    .withColumn("publish_time_key", f.date_format(f.col("publish_time"), "yyyyMMdd"))\
    .withColumn("year", f.year(f.col("publish_time")))\
    .withColumn("month", f.month("publish_time"))\
    .withColumn("day", f.dayofmonth("publish_time"))

# create youtube_fact table
youtube_fact_df = full_data_df\
    .join(category_dim_df, full_data_df.category_title == category_dim_df.category_title, "inner")\
    .join(channel_dim_df, full_data_df.channel_title == channel_dim_df.channel_title, "inner")\
    .join(region_dim_df, full_data_df.region == region_dim_df.region, "inner")\
    .join(video_dim_df, (full_data_df.video_id == video_dim_df.video_id) & (full_data_df.title == video_dim_df.title), "inner")\
    .join(date_dim_df, full_data_df.publish_time == date_dim_df.publish_time, "inner")\
    .select("video_key", "channel_key", "category_key", "region_key", "publish_time_key", "views", "likes", "dislikes", "comment_count")

youtube_fact_df = youtube_fact_df\
    .groupBy("video_key", "channel_key", "category_key", "region_key", "publish_time_key")\
    .agg(f.max("views").alias("views"), f.max("likes").alias("likes"), f.max("dislikes").alias("dislikes"), f.max("comment_count").alias("comment_count"))\

# save category_dim table
category_dim_df = category_dim_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
category_dim_df.toPandas().to_csv(f"{dw_path}/category_dim.csv", index=False)

# save channel_dim table
channel_dim_df = channel_dim_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
channel_dim_df.toPandas().to_csv(f"{dw_path}/channel_dim.csv", index=False)

# save region_dim table
region_dim_df = region_dim_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
region_dim_df.toPandas().to_csv(f"{dw_path}/region_dim.csv", index=False)

# save date_dim table
date_dim_df = date_dim_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
date_dim_df.toPandas().to_csv(f"{dw_path}/date_dim.csv", index=False)

# save video_dim table
video_dim_df = video_dim_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
video_dim_df.toPandas().to_csv(f"{dw_path}/video_dim.csv", index=False)

# save youtube_fact table
youtube_fact_df = youtube_fact_df\
    .withColumn("views", f.col("views").cast("string"))\
    .withColumn("likes", f.col("likes").cast("string"))\
    .withColumn("dislikes", f.col("dislikes").cast("string"))

youtube_fact_df = youtube_fact_df.repartition(1)
dw_path = f"Spark_project/Youtube_Trending/DW_data"
if not os.path.exists(dw_path):
    os.mkdir(dw_path)
youtube_fact_df.toPandas().to_csv(f"{dw_path}/youtube_fact.csv", index=False)


