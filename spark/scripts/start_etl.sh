#!/bin/sh
set -e

echo "ETL Pipeline starting..."
echo "Healthchecks for PG & CH passed by docker-compose"

export PATH="/opt/spark/bin:$PATH"

echo "Step 1/2: DDL - Create Star Schema tables"
spark-submit --master local[*] /opt/spark/scripts/01_transform_raw_to_star.py

echo "Step 2/2: DML - Load CSV data into Star Schema"
spark-submit --master local[*] /opt/spark/scripts/02_create_marts_clickhouse.py

echo "ETL Pipeline completed successfully!"
echo "Container stays alive for log inspection (Ctrl+C to exit)"
tail -f /dev/null
