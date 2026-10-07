from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import DAG, task


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_script(script_name):
    script_path = PROJECT_ROOT / "scripts" / script_name

    subprocess.run(
        ["python", str(script_path)],
        cwd=PROJECT_ROOT,
        check=True,
    )


with DAG(
    dag_id="enterprise_raw_ingestion",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["enterprise", "ingestion", "s3"],
) as dag:

    @task
    def ingest_file_sources():
        run_script("08_ingest_to_s3.py")

    @task
    def ingest_manufacturing_postgres():
        run_script("10_ingest_postgres_to_s3.py")

    file_ingestion = ingest_file_sources()
    manufacturing_ingestion = ingest_manufacturing_postgres()