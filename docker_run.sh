docker container run -d \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e ESPN_SWID=$ESPN_SWID \
    -e ESPN_S2=$ESPN_S2 \
    -e AIRFLOW__CORE__SQL_ALCHEMY_CONN=$AIRFLOW__CORE__SQL_ALCHEMY_CONN \
    airflow:latest