import airflow
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import lib.espn as espn


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@zmsy.co'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 3, 8)
}

dag = airflow.DAG('fantasy_baseball', default_args=default_args)


t1 = PythonOperator(
    dag=dag,
    task_id = 'get_espn_league_data',
    python_callable=espn.get_espn_league_data,
    default_args=default_args
)

t2 = PythonOperator(
    dag=dag,
    task_id = 'get_espn_player_data',
    python_callable=espn.get_espn_player_data,
    default_args=default_args
)

t2.set_upstream(t1)
