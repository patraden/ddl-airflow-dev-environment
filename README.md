### homework
my ddl dev environment

### create all relevant configurations, including db init script
```bash
chmod u+x setup.sh
./setup.sh
```

### init and start airflow
```bash
docker-compose up airflow-init
docker-compose up -d
```