import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import lib.fantasy as fantasy


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@zmsy.co'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 4, 6)
}

dag = airflow.DAG('statcast', default_args=default_args)

t1 = PythonOperator(
    dag=dag,
    task_id = 'get_statcast_batter_actuals',
    python_callable=fantasy.get_statcast_batter_actuals,
    default_args=default_args
)

t2 = PythonOperator(
    dag=dag,
    task_id = 'get_statcast_batter_data',
    python_callable=fantasy.get_statcast_batter_data,
    default_args=default_args
)

t3 = PythonOperator(
    dag=dag,
    task_id = 'get_statcast_pitcher_actuals',
    python_callable=fantasy.get_statcast_pitcher_actuals,
    default_args=default_args
)

t2.set_upstream(t1)
t2.set_upstream(t3)


