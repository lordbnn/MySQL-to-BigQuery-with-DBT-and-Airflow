# Airflow Data Pipeline with MySQL to BigQuery

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

This repository contains code for an Apache Airflow data pipeline that moves data from a MySQL database to Google BigQuery. The pipeline consists of two DAGs: one for a full load and another for an incremental load. The gender column in the employees table is transformed using a DBT model to map 'M' to 'Male' and 'F' to 'Female'.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [DAGs](#dags)
- [DBT Model](#dbt-model)
- [CI/CD Pipeline](#cicd-pipeline)


## Getting Started

To use this data pipeline, follow the steps below:

1. Set up the Employees sample database in MySQL. (Reference: [MySQL Documentation](https://dev.mysql.com/doc/employee/en/))
2. Install Apache Airflow and configure the necessary connections for MySQL and BigQuery.
3. Create a DBT project and set up the required model to transform the gender column.
4. Host this code on your Airflow server.

## DAGs

### Full Load DAG

The Full Load DAG (`full_load_dag.py`) moves all data from the employees table in MySQL to BigQuery. It runs on-demand.

### Incremental Load DAG

The Incremental Load DAG (`incremental_load_dag.py`) moves data daily and contains only changes that have happened in the past day. It runs daily.

## DBT Model

The DBT project contains a model named `employees` that transforms the gender column in the employees table. It maps 'M' to 'Male' and 'F' to 'Female'.

The SQL code for the model can be found in `dbt/models/employees.sql`.

## CI/CD Pipeline

I've set up a CI/CD pipeline using GitHub Actions to automatically deploy new code changes to our Airflow environment. The pipeline triggers on every push to the `main` branch.


