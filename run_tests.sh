#!/bin/bash
set -e

export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__UNIT_TEST_MODE=True

echo "Running DAG integrity tests..."
pytest tests/test_dag_integrity.py -v