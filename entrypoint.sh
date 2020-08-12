pipenv shell
airflow initdb
airflow scheduler &
sleep 10
exec airflow webserver -p 8080
