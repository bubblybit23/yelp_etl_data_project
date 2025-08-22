

### **Phase 0: Project Foundation & Data Acquisition**
**Goal:** Create the project skeleton and acquire the raw data.
**Success Token:** `[PHASE_0_COMPLETE]` - Raw data is in place and Git is secured.

1.  **Create Project Structure:**
    ```bash
    mkdir -p yelp-data-platform && cd yelp-data-platform
    mkdir -p data/raw airflow/dags airflow/plugins spark/jobs dbt/yelp_transform/models/staging dbt/yelp_transform/models/marts dbt/yelp_transform/tests infrastructure scripts outputs docs
    ```
2.  **Initialize Git and Environment:**
    ```bash
    git init
    python -m venv .venv
    source .venv/bin/activate  # Linux/Mac
    # .\.venv\Scripts\activate  # Windows
    ```
3.  **Place Raw Data:** Manually download and extract the Kaggle dataset. Place `yelp_academic_dataset_business.json` and `yelp_academic_dataset_review.json` into the `data/raw/` directory.
4.  **Create Critical Config Files:**
    *   `.gitignore`: Add `/data/`, `.venv/`, `outputs/`
    *   `requirements.txt`: Add `apache-airflow==2.7.3 dbt-core==1.5.10 kaggle`

**Output:** A project directory with raw data, ready for infrastructure setup.
**Token for Next Phase:** `[PHASE_0_COMPLETE]`

---

### **Phase 1: Infrastructure Provisioning (Docker)**
**Goal:** Define and launch the core platform services.
**Success Token:** `[PHASE_1_COMPLETE]` - All containers are healthy and UIs are accessible.
**Trigger:** `[PHASE_0_COMPLETE]`

1.  **Create `infrastructure/docker-compose.yml`** (using the provided, aligned YAML).
2.  **Create `airflow/requirements.txt`** for Airflow providers.
    ```bash
    echo "apache-airflow-providers-apache-spark==4.1.1
    apache-airflow-providers-apache-hive==6.1.1" > airflow/requirements.txt
    ```
3.  **Launch the Platform:**
    ```bash
    cd infrastructure
    docker compose up -d --build
    ```
4.  **Verify Health:** Use `docker ps` and check UIs at `localhost:8080` (Airflow) and `localhost:9090` (Spark).

**Output:** A running Docker environment with Airflow, Spark, and Hive.
**Token for Next Phase:** `[PHASE_1_COMPLETE]`

----------- 
### **PHASE 1 : Confimation and returning to work**
# 1. Open VS Code and your project terminal.
# 2. Reactivate your virtual environment
source .venv/bin/activate  # Linux/Mac
# .\.venv\Scripts\activate  # Windows

# 3. Rebuild and launch your platform
cd infrastructure
docker compose up -d --build

# 4. Wait for the containers to become healthy (check with 'docker ps')
# 5. Re-validate the key connection (this is quick)
docker exec -it spark-master /opt/bitnami/spark/bin/spark-shell --conf spark.sql.catalogImplementation=hive -e "spark.sql(\"SHOW DATABASES;\").show()"
# showing 'default' in the table

---

### **Phase 2: Data Ingestion (PySpark)**
**Goal:** Ingest raw JSON into Hive as optimized ORC tables.
**Success Token:** `[PHASE_2_COMPLETE]` - Hive tables `yelp_raw.reviews` and `yelp_raw.businesses` are created and queryable.
**Trigger:** `[PHASE_1_COMPLETE]`

1.  **Write `spark/jobs/process_yelp_data.py`** (as defined in your structure).
2.  **Test the Job Manually** by executing it inside the Spark container:
    ```bash
    # Get into the Spark master container
    docker exec -it spark-master bash
    # Run the job from inside the container
    /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/process_yelp_data.py
    ```
3.  **Validate in Hive:** Connect to Hive and verify the tables.
    ```bash
    docker exec -it hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n hive
    USE yelp_raw;
    SHOW TABLES;
    SELECT * FROM reviews LIMIT 5;
    ```

**Output:** Processed, queryable data in the data warehouse (Hive).
**Token for Next Phase:** `[PHASE_2_COMPLETE]`

---

### **Phase 3: Data Modeling & Transformation (dbt)**
**Goal:** Build a tested, documented analytics layer on top of the raw tables.
**Success Token:** `[PHASE_3_COMPLETE]` - dbt models run successfully and all data tests pass.
**Trigger:** `[PHASE_2_COMPLETE]`

1.  **Initialize dbt Project:** Navigate to `dbt/yelp_transform` and run `dbt init . --skip-profile-setup`. Edit `dbt_project.yml` and `profiles.yml` to connect to Hive.
2.  **Create Sources:** Define `models/staging/src_yelp.yml` to declare `yelp_raw.reviews` and `yelp_raw.businesses` as dbt sources.
3.  **Build Models:**
    *   Write `models/staging/stg_reviews.sql` and `stg_businesses.sql`.
    *   Write `models/marts/dim_business.sql` and `fact_reviews.sql`.
4.  **Implement Data Tests:** Create `tests/not_null_unique_tests.yml`.
5.  **Run dbt:** Execute `dbt run` and then `dbt test` from within the `dbt/yelp_transform` directory.

**Output:** A transformed, analytics-ready data model with integrity checks.
**Token for Next Phase:** `[PHASE_3_COMPLETE]`

---

### **Phase 4: Orchestration & Automation (Airflow)**
**Goal:** Automate the entire pipeline from raw data to analytics-ready data.
**Success Token:** `[PHASE_4_COMPLETE]` - The Airflow DAG runs successfully without manual intervention.
**Trigger:** `[PHASE_3_COMPLETE]`

1.  **Write the Master DAG:** Create `airflow/dags/yelp_data_pipeline.py` with three tasks: `spark_submit_task` -> `dbt_run_task` -> `dbt_test_task`.
2.  **Add DAG to Airflow:** The Docker volume mount will automatically add it to the Airflow scheduler.
3.  **Trigger the DAG:** Manually trigger the DAG from the Airflow UI (`localhost:8080`).
4.  **Monitor:** Watch the Airflow UI to ensure all tasks turn green.

**Output:** A fully automated, production-grade data pipeline.
**Token for Next Phase:** `[PHASE_4_COMPLETE]`

---

### **Phase 5: Analysis & Documentation**
**Goal:** Prove the pipeline works and create a compelling portfolio entry.
**Success Token:** `[PROJECT_COMPLETE]` - GitHub repository is live with a comprehensive README and results.
**Trigger:** `[PHASE_4_COMPLETE]`

1.  **Run Analytical Queries:** Use the scripts in `scripts/` to run queries on the final marts. Save outputs to `outputs/`.
2.  **Document Everything:** Write a detailed `README.md` explaining each phase, the technology choices, and how to run the project.
3.  **Final Git Commit:**
    ```bash
    git add .
    git commit -m "feat: complete end-to-end data pipeline with Airflow, Spark, dbt, and Hive"
    ```
4.  **Push to GitHub.**

**Final Output:** A complete, portfolio-ready data engineering project.
**Final Token:** `[PROJECT_COMPLETE]`

This phased, token-based approach ensures each part of the system is built and validated before moving on to the next, creating a robust final product and clearly demonstrating your methodical engineering process.
