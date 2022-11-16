### homework
my ddl dev environment

### create all relevant configurations, including dwh init script
```bash
chmod u+x setup.sh
./setup.sh
```
### flush previous setups
```bash
docker-compose down --rmi all --remove-orphans
docker volume prune -f
```
### build and start airflow
```bash
docker build -t extended-clickhouse:latest -f clickhouse.Dockerfile .
docker build -t extended-airflow:latest -f airflow.Dockerfile .
docker-compose up airflow-init
docker-compose up -d
```