"""U2 Activity-2 (Part 1): Orchestrating ETL Pipelines with Apache Airflow.

Orchestration skeleton for a linear ETL chain:

    start -> extract_data -> validate_data -> transform_data -> load_data

Each task intentionally contains only print statements so the focus stays on
workflow behavior: DAG structure, dependencies, DAG Runs, Task Instances,
execution states, logs, and failure propagation.

Do NOT run this file with ``python reliable_pipeline_demo.py``. Airflow's DAG
processor discovers and parses this file from the mounted ``dags/`` folder,
and Airflow -- not a direct Python command -- manages execution.

validate_data is a SIMULATION. It does not run Great Expectations. Part 2 of
the activity replaces it with a real automated validation contract.
"""

import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="reliable_pipeline_demo",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "guided-practice"],
)
def reliable_pipeline_demo():
    @task
    def start():
        print("Pipeline started.")

    @task
    def extract_data():
        print("Extracting data...")
        print("Extraction completed successfully.")

    @task
    def validate_data():
        print("Validating incoming data...")
        print("Raw data validation passed.")

    @task
    def transform_data():
        print("Transforming data...")
        print("Transformation completed successfully.")

    @task
    def load_data():
        print("Loading data...")
        print("Load completed successfully.")

    start_task = start()
    extract_task = extract_data()
    validate_task = validate_data()
    transform_task = transform_data()
    load_task = load_data()

    (
        start_task
        >> extract_task
        >> validate_task
        >> transform_task
        >> load_task
    )


reliable_pipeline_demo()