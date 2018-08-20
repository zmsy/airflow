import airflow
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import lib.fantasy


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@zmsy.co'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = airflow.DAG('fantasy_baseball', default_args=default_args)


t1 = PythonOperator(
    dag=dag,
    task_id = 'collect_data',
    python_callable=fantasy.main
)
