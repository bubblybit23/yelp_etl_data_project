# Phase 3 Challenges Documentation: dbt Connection Issues

## Overview
During Phase 3 of the Yelp ETL Data Project, we encountered significant challenges establishing a connection between dbt (data build tool) and the Spark cluster running in Docker containers. Despite successful data processing through Spark jobs, the dbt integration faced networking hurdles.

## Environment Setup
- **dbt Version**: 1.10.9
- **Python Version**: 3.11.7
- **OS**: Windows 10
- **Adapter**: dbt-spark 1.9.3
- **Infrastructure**: Docker containers (spark-master, spark-worker, airflow-postgres)
- **Network**: Custom Docker network `infrastructure_yelp-platform-net`

## Successful Spark Data Processing

### Spark Transformation Pipeline
We have a fully functional Spark-based ETL pipeline that successfully processes Yelp data:

#### **1. Staging Layer Creation** (`spark/jobs/create_staging_layer.py`)
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/create_staging_layer.py
```

#### **2. Transformation Layer Creation** (`spark/jobs/create_transformation_layer.py`)
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/create_transformation_layer.py
```

### **Successful Output Results**
```
Scaling row group sizes to 95.00% for 8 writers
Written 6,990,280 records to fact_reviews
>>> Running data quality checks...
✅ dim_business count: 150,346
✅ fact_reviews count: 6,990,280
✅ Null business_id count: 0
>>> Transformation layer creation completed successfully!
>>> Sample from dim_business:
+----------------------+-----------------------------------------------------+-----------+-----+----------+
|business_id           |name                                                 |city       |state|avg_rating|
+----------------------+-----------------------------------------------------+-----------+-----+----------+
|--7PUidqRWpRSpXebiyxTg|Humpty's Family Restaurant                           |Edmonton   |AB   |1.75      |
|--S43ruInmIsGrnnkmavRw|Peaches Records                                      |New Orleans|LA   |3.35      |
|--eBbs3HpZYIym5pE8Qdw|Holiday Inn Express & Suites Tampa-Fairgrounds-Casino|Tampa      |FL   |2.56      |
|-0oPt7sSKtJG1ysLwV_E9g|Big Lots                                             |Tampa      |FL   |3.8       |
|-1-8eimDEnS9fezJNZkQkQ|Wire To Wire                                         |Havertown  |PA   |4.2       |
+----------------------+-----------------------------------------------------+-----------+-----+----------+
```

### **Data Quality Metrics**
- ✅ **dim_business**: 150,346 records
- ✅ **fact_reviews**: 6,990,280 records  
- ✅ **Data Integrity**: 0 null business_id values
- ✅ **Quality Checks**: All validation tests passed

## dbt Integration Challenges

Despite successful Spark processing, dbt connection issues persist:

### 1. **Host Resolution Failure**
**Error Message**: 
```
failed to resolve sockaddr for spark-master:10001
socket.gaierror: [Errno 11001] getaddrinfo failed
```

**Root Cause**: 
- dbt on Windows host cannot resolve Docker container hostnames
- Container name `spark-master` only resolvable within Docker network

### 2. **Connection Timeout with IP Address**
**Error Message**:
```
Could not connect to any of [('172.18.0.4', 10001)]
```

**Root Cause**:
- Connection timeout despite correct IP address
- Possible Thrift server configuration issues

## Current Architecture Status

### ✅ **Working Components**
1. **Spark Cluster**: Fully operational in Docker
2. **Data Processing**: Staging and transformation layers working
3. **Data Quality**: All validation checks passing
4. **Volume**: 7M+ records processed successfully

### ❌ **Blocking Issues**
1. **dbt-Spark Connection**: Networking configuration issues
2. **Host-Container Communication**: Windows-Docker networking challenges
3. **Thrift Server Access**: Cannot connect from host machine

## Troubleshooting Steps Taken

### Step 1: Network Configuration
```bash
docker network inspect infrastructure_yelp-platform-net
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Networks}}"
```

**Findings**: Containers on custom network with assigned IPs

### Step 2: Connection Testing
Multiple `profiles.yml` configurations attempted:
1. `host: spark-master` ❌ (DNS resolution failed)
2. `host: 172.18.0.4` ❌ (Connection timeout)
3. `host: localhost` ❌ (Port not exposed)

### Step 3: Dockerized dbt Approach
```powershell
docker run -it --rm `
  --network infrastructure_yelp-platform-net `
  -v ${PWD}/dbt:/usr/app/dbt `
  -v ${HOME}/.dbt:/root/.dbt `
  --name dbt-runner `
  ghcr.io/dbt-labs/dbt-spark:1.10.9 `
  dbt run
```

**Challenge**: Docker image availability issues

## Workaround: Spark-to-Local Export Strategy

Since Spark processing is working perfectly, we can implement a workaround:

### Option A: Export from Spark to Local Database
```python
# Add to transformation script:
df.write.parquet("/output/transformed_data.parquet")
# or
df.write.jdbc(url="jdbc:sqlite:yelp.db", table="dim_business")
```

### Option B: Use Spark SQL Temporary Views
```python
# Create temporary views in Spark
df.createOrReplaceTempView("dim_business")
```

### Option C: Hybrid Approach
1. **Process**: Use Spark for heavy transformation
2. **Serve**: Export to local SQLite for dbt development
3. **Deploy**: Use Spark for production dbt runs

## Key Insights

### What's Working Well
- ✅ Spark data processing pipeline is fully functional
- ✅ Data quality and validation checks are passing
- ✅ Large-scale data processing (7M+ records) successful
- ✅ Docker infrastructure is stable and operational

### What Needs Fixing
- ❌ Cross-platform networking (Windows ↔ Docker)
- ❌ Thrift server configuration and exposure
- ❌ dbt adapter connectivity

## Recommended Solutions

### Immediate (Development)
1. **Export Data**: Spark → Parquet → Local SQLite/DuckDB
2. **Develop Locally**: Use local DB for dbt development
3. **Test Logic**: Validate dbt models with sample data

### Medium-term (Integration)
1. **Port Forwarding**: Expose Thrift server to host
2. **Network Bridge**: Proper Docker network configuration
3. **Image Availability**: Resolve dbt-spark image issues

### Long-term (Production)
1. **Unified Platform**: Run dbt in same Docker environment
2. **CI/CD Pipeline**: Automated testing and deployment
3. **Monitoring**: Comprehensive logging and alerting

## Conclusion

**The data transformation pipeline is fundamentally sound** - we've successfully processed 7M+ records with full data quality validation. The current challenge is purely a networking/integration issue between dbt and the Spark cluster, not a data processing issue.

The Spark jobs have already demonstrated that the core transformation logic works correctly, producing the expected dimensional model structure that dbt would leverage.