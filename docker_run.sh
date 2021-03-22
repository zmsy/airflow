docker image build -t airflow .
docker container run -d --env-file .env -p 8080:8080 airflow:latest
