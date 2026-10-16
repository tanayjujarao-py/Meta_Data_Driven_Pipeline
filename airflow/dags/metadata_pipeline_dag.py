from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="metadata_driven_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["metadata", "snowflake", "s3"],
)
def metadata_driven_pipeline():

    @task
    def run_metadata_pipeline():

        from ingestion.run_pipeline import run_pipeline

        run_pipeline()

    run_metadata_pipeline()


metadata_driven_pipeline()
