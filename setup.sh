#/bin/bash

# Exercise enviroment preset for docker-compose.yaml

set -e

# flush previous setups if exists
rm -Rf ./dags ./logs ./plugins ./init.dwh ./storage
rm -f .env

# setup docker-compose volumes
mkdir -p ./dags ./logs ./plugins ./init.dwh ./storage

# setup airflow enviroment variables
echo "AIRFLOW_UID=$(id -u)" > .env
echo "CONN_TEST_DB='{\"conn_type\": \"postgres\", \"login\": \"$(echo $TEST_DB_USER)\", \"password\": \"$(echo $TEST_DB_PASSWORD)\", \"host\": \"$(echo $TEST_DB_HOST)\", \"port\": $(echo $TEST_DB_PORT), \"schema\": \"$(echo $TEST_DB_NAME)\", \"extra\": \"\"}'" >> .env
echo "CONN_EXCHANGERATE_HOST_API='{\"conn_type\": \"http\", \"host\": \"https://api.exchangerate.host\", \"schema\": \"\", \"extra\": \"\"}'" >> .env
echo "VAR_EXCHANGERATE_HOST_PAIR='{\"base\" : \"BTC\", \"code\" : \"USD\" }'" >> .env
echo "VAR_EXCHANGERATE_HOST_HISTORY_LOAD=1" >> .env
echo "VAR_EXCHANGERATE_HOST_HISTORY_START='2010-01-01'" >> .env
echo "VAR_FILE_STORAGE_MOUNT_POINT='/tmp/storage'" >> .env
echo "_PIP_ADDITIONAL_REQUIREMENTS='clickhouse-driver==0.2.4'" >> .env

# generate table creation script
# cat << EOF > ./init.dwh/create_exchange_rates_table.sh
# #!/bin/bash
# set -e
# psql -v ON_ERROR_STOP=1 --username "\$POSTGRES_USER" --dbname "\$POSTGRES_DB" << EOSQL
#     CREATE TABLE IF NOT EXISTS EXCHANGE_RATES (
#                 base CHAR(3) NOT NULL, 
#                 code CHAR(3) NOT NULL, 
#                 date DATE NOT NULL,
#                 rate real NULL,
#                 __dag_id__ CHARACTER VARYING NOT NULL,
#                 __dag_run_id__ CHARACTER VARYING NOT NULL,
#                 __dag_run_start_date__ TIMESTAMP NOT NULL
#             );
# EOSQL
# EOF