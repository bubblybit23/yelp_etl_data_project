from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="yelp_data_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["yelp", "spark", "dbt"],
) as dag:
    # Task to run the Spark job that creates the staging layer
    spark_staging_task = SparkSubmitOperator(
        task_id="spark_create_staging_layer",
        application="/opt/bitnami/spark/jobs/create_staging_layer.py",
        conn_id="spark_default",
        master="spark://spark-master:7077",
        conf={"spark.driver.memory": "4g", "spark.executor.memory": "4g"},
        verbose=True,
    )

    # Task to run the Spark job that creates the transformation layer (dims and facts)
    spark_transformation_task = SparkSubmitOperator(
        task_id="spark_create_transformation_layer",
        application="/opt/bitnami/spark/jobs/create_transformation_layer.py",
        conn_id="spark_default",
        master="spark://spark-master:7077",
        conf={"spark.driver.memory": "4g", "spark.executor.memory": "4g"},
        verbose=True,
    )

    # Task to run dbt build (runs models and tests) using the DockerOperator
    dbt_build_task = DockerOperator(
        task_id="dbt_build",
        image="infrastructure_dbt",  # This is the image name created by docker-compose
        api_version="auto",
        auto_remove=True,
        command=["build"],
        docker_url="unix://var/run/docker.sock",
        network_mode="yelp-platform-net",  # Connect to the same network as Spark and Hive
        mount_tmp_dir=False,
        # The working directory inside the container must be where dbt_project.yml is
        working_dir="/usr/app",
    )

    # Define the task dependencies
    spark_staging_task >> spark_transformation_task >> dbt_build_task
