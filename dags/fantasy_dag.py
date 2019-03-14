import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import lib.espn as espn
import lib.fantasy as fantasy


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

t3 = PythonOperator(
    dag=dag,
    task_id = 'load_league_members_to_postgres',
    python_callable=espn.load_league_members_to_postgres,
    default_args=default_args
)

t4 = PythonOperator(
    dag=dag,
    task_id = 'load_teams_to_postgres',
    python_callable=espn.load_league_members_to_postgres,
    default_args=default_args
)

t5 = PythonOperator(
    dag=dag,
    task_id="get_all_fangraphs_projections",
    python_callable=fantasy.get_all_fangraphs_pages,
    default_args=default_args
)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t2)
t5.set_upstream(t1)
