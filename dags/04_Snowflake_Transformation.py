from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'alfa_projekt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '04_Snowflake_Transformation_dbt',
    default_args=default_args,
    description='Spouští dbt transformace (Silver & Gold) v Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt', 'dbt', 'snowflake']
) as dag:

    # Task pro spuštění dbt run
    # export DBT_PROFILES_DIR zajistí, že Airflow najde tvůj profiles.yml
    run_dbt = BashOperator(
        task_id='dbt_run_all_models',
        bash_command=(
            "export DBT_PROFILES_DIR=/opt/airflow && "
            "cd /opt/airflow/dbt_alfa && "
            "dbt run"
        )
    )

    # Task pro dbt testy (kontrola unikátnosti ID, null hodnot atd.)
    run_tests = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=(
            "export DBT_PROFILES_DIR=/opt/airflow && "
            "cd /opt/airflow/dbt_alfa && "
            "dbt test"
        )
    )

    run_dbt >> run_tests
