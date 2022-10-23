## homework
my ddl dev enviroment

# create all relevant configurations, including db init script
chmod u+x setup.sh
./setup.sh

# init and start airflow
docker-compose up airflow-init
docker-compose up -d
```