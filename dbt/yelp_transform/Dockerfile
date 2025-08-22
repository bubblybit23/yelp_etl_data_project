# Base image
FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app

# Install dbt and dbt-spark adapter
# We specify versions for reproducibility
RUN pip install dbt-core==1.5.10 dbt-spark==1.5.1 pyspark==3.5.0

# Copy dbt project files
COPY . .

# Set the DBT_PROFILES_DIR to our project directory
# This tells dbt where to look for the profiles.yml file
ENV DBT_PROFILES_DIR=.
