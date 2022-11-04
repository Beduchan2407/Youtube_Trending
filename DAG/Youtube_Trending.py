import os
from datetime import datetime, date
from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import date, datetime

region_list = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

with DAG(
    dag_id="Youtube_Trending",
    start_date=datetime(year=2022, month=11, day=4),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Start task
    task_start = DummyOperator(
        task_id = "Start",
    )

    # End task
    task_end = DummyOperator(
        task_id = "End",
    )

    # Processing data task
    task_processing_data = SparkSubmitOperator(
        task_id = f"Processing_data",
        conn_id="spark_local",
        application="Spark_project/Youtube_Trending/Spark/processing_data.py",
        trigger_rule = "all_done"
    )

    # Clean data task
    for region in region_list:
        task_clean_data = SparkSubmitOperator(
            task_id = f"Clean_data_region_{region}",
            conn_id="spark_local",
            application="Spark_project/Youtube_Trending/Spark/clean_data.py",
            application_args=[f"{region}"]
        )

        task_start >> task_clean_data >> task_processing_data

    # Create tables task
    task_create_table = PostgresOperator(
        task_id = "Create_tables",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            create table if not exists category_dim(
                category_title varchar(255) not null,
                category_key int primary key not null
            );

            create table if not exists channel_dim(
                channel_title varchar(255) not null,
                channel_key int primary key not null
            );

            create table if not exists date_dim(
                publish_time date not null,
                publish_time_key int primary key not null,
                year int not null,
                month int not null,
                day int not null
            );

            create table if not exists region_dim(
                region varchar(255) not null,
                region_key int primary key not null
            );

            create table if not exists video_dim(
                video_id varchar(255) not null,
                video_title varchar(255) not null,
                video_key int primary key not null
            );

            create table if not exists youtube_fact(
                video_key int not null,
                channel_key int not null,
                category_key int not null,
                region_key int not null,
                publish_time_key int not null,
                views int not null,
                likes int not null,
                unlikes int not null,
                comment_count int not null,
                primary key (video_key, channel_key, category_key, region_key, publish_time_key)
            )
        """
    )

    # Load category_dim table
    task_load_category_dim = PostgresOperator(
        task_id = "Load_category_dim_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from category_dim;

            copy category_dim(category_title, category_key)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/category_dim.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Load channel_dim table
    task_load_channel_dim = PostgresOperator(
        task_id = "Load_channel_dim_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from channel_dim;

            copy channel_dim(channel_title, channel_key)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/channel_dim.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Load date_dim table
    task_load_date_dim = PostgresOperator(
        task_id = "Load_date_dim_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from date_dim;

            copy date_dim(publish_time, publish_time_key, year, month, day)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/date_dim.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Load region_dim table
    task_load_region_dim = PostgresOperator(
        task_id = "Load_region_dim_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from region_dim;

            copy region_dim(region, region_key)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/region_dim.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Load video_dim table
    task_load_video_dim = PostgresOperator(
        task_id = "Load_video_dim_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from video_dim;

            copy video_dim(video_id, video_title, video_key)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/video_dim.csv'
            delimiter ','
            csv HEADER
        """
    )

    # Load youtube_fact table
    task_load_youtube_fact = PostgresOperator(
        task_id = "Load_youtube_fact_table",
        postgres_conn_id="Youtube_Trending_DW",
        sql = """
            delete from youtube_fact;

            copy youtube_fact(video_key, channel_key, category_key, region_key, publish_time_key, views, likes, unlikes, comment_count)
            from '/home/minhhieu/Spark_project/Youtube_Trending/DW_data/youtube_fact.csv'
            delimiter ','
            csv HEADER
        """,
        trigger_rule = "all_done"
    )

    task_processing_data >> task_create_table
    task_create_table >> [task_load_category_dim, task_load_channel_dim, task_load_video_dim, task_load_date_dim, task_load_region_dim] >> task_load_youtube_fact >> task_end







