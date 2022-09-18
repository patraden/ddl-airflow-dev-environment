## homework

## setup
```bash
# assume that docker and docker-compose requirements have been met
# https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
export TEST_DB_HOST=<...>
export TEST_DB_PORT=<...>
export TEST_DB_NAME=<...>
export TEST_DB_USER=<...>
export TEST_DB_PASSWORD=<...>
chmod u+x setup.sh
./setup.sh
docker-compose up airflow-init
docker-compose up -d
```