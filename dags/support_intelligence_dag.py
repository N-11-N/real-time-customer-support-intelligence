"""Airflow DAG for production deployment; the Colab notebook runs the same stages interactively."""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.pipeline_tasks import (
    ingest_and_validate,
    validate_quality,
    build_lakehouse,
    refresh_rag_index,
    emit_complete,
)


@dag(
    dag_id="customer_support_intelligence",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["capstone", "kafka", "delta", "rag"],
)
def support_intelligence_pipeline():
    @task
    def ingest_kafka():
        return ingest_and_validate()

    @task
    def run_quality_gate(metadata):
        return validate_quality(metadata)

    @task
    def merge_bronze_silver_gold(metadata):
        return build_lakehouse(metadata)

    @task
    def refresh_vector_index(metadata):
        return refresh_rag_index(metadata)

    @task
    def emit_lineage(metadata):
        return emit_complete(metadata)

    emit_lineage(refresh_vector_index(merge_bronze_silver_gold(
        run_quality_gate(ingest_kafka()))))


support_intelligence_pipeline()
