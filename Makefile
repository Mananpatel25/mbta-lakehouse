.PHONY: up down producer consume gates dbt duck test backfill
up:        ; docker compose up -d
down:      ; docker compose down -v
producer:  ; MBTA_API_KEY=$$(grep MBTA_API_KEY .env | cut -d= -f2) python ingestion/producer.py
consume:   ; S3_ENDPOINT=http://localhost:9000 python consumer/consumer.py
gates:     ; S3_ENDPOINT_HOST=localhost:9000 python quality/gates.py
dbt:       ; cd dbt && DUCKDB_PATH=lakehouse.duckdb S3_ENDPOINT_HOST=localhost:9000 dbt build --profiles-dir .
duck:      ; duckdb dbt/lakehouse.duckdb
test:      ; pytest -q && ruff check .
backfill:  ; docker compose exec airflow-scheduler airflow dags backfill mbta_lakehouse -s $(START) -e $(END)
