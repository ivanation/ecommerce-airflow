from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

def on_failure_callback(context):
    """Función que se ejecuta automáticamente si cualquier tarea del DAG falla"""
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    error_msg = context.get('exception')
    
    # Aquí es donde normalmente enviarías un Slack o un Email
    print(f"🚨 ALERTA CRÍTICA: El DAG {dag_id} falló en la tarea {task_id}!")
    print(f"❌ ERROR: {error_msg}")

# Argumentos por defecto para todas las tareas del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'ecommerce_daily_pipeline',
    default_args=default_args,
    description='Pipeline completo de E-commerce: Ingesta + Transformación (dbt)',
    schedule_interval='@daily', # Se ejecuta una vez al día a medianoche
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=on_failure_callback, # Activamos la alerta para todo el DAG
    tags=['ecommerce', 'dbt', 'ingestion'],
) as dag:

    # ==========================================
    # Tarea 1: Ingesta de datos (Python)
    # ==========================================
    # Llama al script que adaptamos para ejecutarse dentro del contenedor
    task_ingest_data = BashOperator(
        task_id='ingest_raw_data',
        bash_command='python /opt/airflow/scripts/ingest.py',
    )

    # ==========================================
    # Tarea 2: Transformación (dbt run)
    # ==========================================
    # Ejecuta toda la lógica de limpieza y uniones
    task_dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='dbt run --project-dir /opt/airflow/scripts/ecommerce_pipeline --profiles-dir /opt/airflow/scripts/ecommerce_pipeline',
    )

    # ==========================================
    # Tarea 3: Calidad de datos (dbt test)
    # ==========================================
    # Asegura que no hay duplicados ni nulos después de transformar
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality',
        bash_command='dbt test --project-dir /opt/airflow/scripts/ecommerce_pipeline --profiles-dir /opt/airflow/scripts/ecommerce_pipeline',
    )

    # ==========================================
    # Orquestación (Definir el orden)
    # ==========================================
    # El flujo es: Ingesta -> Transformación -> Tests
    task_ingest_data >> task_dbt_run >> task_dbt_test
