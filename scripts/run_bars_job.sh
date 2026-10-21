#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
export PYTHONPATH="$PWD"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
exec "${SPARK_HOME:-$HOME/spark/spark-4.2.0-bin-hadoop3}/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  spark_jobs/bars_job.py "$@"