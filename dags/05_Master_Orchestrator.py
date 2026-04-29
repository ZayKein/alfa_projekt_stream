from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'alfa_projekt',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    '05_Master_Orchestrator',
    default_args=default_args,
    description='Hlavní dirigent: Robustní sekvenční spouštění celé platformy',
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt', 'master', 'production']
) as dag:

    # Společné nastavení pro stabilitu na Local/Sequential Executoru
    t_config = {
        'wait_for_completion': True,
        'reset_dag_run': True,
        'poke_interval': 5
    }

    t00 = TriggerDagRunOperator(
        task_id='trig_00_hr',
        trigger_dag_id='00_hr_generator',
        **t_config
    )

    t01a = TriggerDagRunOperator(
        task_id='trig_01a_products',
        trigger_dag_id='01_A_Products_Test',
        **t_config
    )

    t01b = TriggerDagRunOperator(
        task_id='trig_01b_traffic',
        trigger_dag_id='01_B_Traffic_Final',
        **t_config
    )

    t01c = TriggerDagRunOperator(
        task_id='trig_01c_orders',
        trigger_dag_id='01_C_Orders_Final',
        **t_config
    )

    t02 = TriggerDagRunOperator(
        task_id='trig_02_load_pg',
        trigger_dag_id='02_Load_All_To_Postgres',
        **t_config
    )

    t03 = TriggerDagRunOperator(
        task_id='trig_03_load_sf',
        trigger_dag_id='03_Postgres_To_Snowflake',
        **t_config
    )

    t04 = TriggerDagRunOperator(
        task_id='trig_04_dbt_transform',
        trigger_dag_id='04_Snowflake_Transformation_dbt',
        **t_config
    )

    t06 = TriggerDagRunOperator(
        task_id='trig_06_ml_predictions',
        trigger_dag_id='06_ML_Predictions',
        **t_config
    )

    # --- LINEÁRNÍ TOK ---
    t00 >> t01a >> t01b >> t01c >> t02 >> t03 >> t04 >> t06
