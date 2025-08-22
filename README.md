# Yelp Data Platform

This project implements an end-to-end data pipeline for processing Yelp dataset, using a modern, containerized architecture with Docker, Spark, dbt, and Airflow.

## Project Overview

The goal of this project is to ingest raw Yelp data, process it into a structured format, transform it into a data model suitable for analytics, and orchestrate the entire workflow.

The pipeline follows these major steps:
1.  **Data Ingestion:** Raw JSON data is ingested and stored in a Hive data warehouse using Spark.
2.  **Data Transformation:** The raw data is transformed into a staging layer and then into a final dimensional model (marts) using Spark.
3.  **Data Modeling & Testing:** dbt is used to build and test the analytical models on top of the transformed data.
4.  **Orchestration:** Airflow is used to orchestrate the entire pipeline, from data ingestion to dbt model execution.

## Architecture

The entire platform is containerized using Docker and defined in the `infrastructure/docker-compose.yml` file. This ensures a reproducible and portable environment. The main services are:

*   **Spark:** Used for distributed data processing (ingestion and transformation).
*   **Hive:** Acts as the data warehouse, storing the data in a structured format.
*   **dbt:** Used for data modeling and transformation within the data warehouse. It runs in its own container and connects to the Hive Thrift Server.
*   **Airflow:** Orchestrates the entire pipeline, triggering Spark jobs and dbt runs.
*   **PostgreSQL:** Serves as the metadata database for Airflow and Hive.

## How to Run the Pipeline

1.  **Prerequisites:**
    *   Docker and Docker Compose installed.
    *   Raw Yelp data files (`yelp_academic_dataset_business.json` and `yelp_academic_dataset_review.json`) placed in the `data/raw` directory.

2.  **Build and Start the Services:**
    ```bash
    docker-compose -f infrastructure/docker-compose.yml up --build -d
    ```

3.  **Trigger the Airflow DAG:**
    *   Open the Airflow UI in your browser at `http://localhost:8080`.
    *   Log in with username `airflow` and password `airflow`.
    *   Find the `yelp_data_pipeline` DAG and trigger it manually.

4.  **Access the Data:**
    *   You can query the final tables in the `yelp_marts` database using any tool that can connect to HiveServer2 (e.g., DBeaver, a Python script with PyHive). The connection details are:
        *   Host: `localhost`
        *   Port: `10000`
        *   Database: `yelp_marts`

## dbt Project

The dbt project is located in `dbt/yelp_transform`. It contains the models for the staging and mart layers, as well as data quality tests. The models are run as part of the Airflow DAG. To run dbt commands manually, you can exec into the dbt container:

```bash
docker exec -it dbt bash
dbt run
dbt test
```
