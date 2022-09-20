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
echo "CONN_DWH='{\"conn_type\": \"sqlite\", \"host\": \"dwh\", \"schema\": \"dwh\", \"extra\": \"\"}'" >> .env
echo "VAR_EXCHANGERATE_HOST_PAIR='{\"base\" : \"BTC\", \"code\" : \"USD\" }'" >> .env
echo "VAR_EXCHANGERATE_HOST_HISTORY_LOAD=1" >> .env
echo "VAR_EXCHANGERATE_HOST_HISTORY_START='2010-01-01'" >> .env
echo "VAR_FILE_STORAGE_MOUNT_POINT='/tmp/storage'" >> .env
echo "_PIP_ADDITIONAL_REQUIREMENTS='airflow-clickhouse-plugin==0.8.2'" >> .env

# generate dwh init script
cat << EOF > ./init.dwh/create_dwh_objects.sh
#!/bin/bash
set -e
clickhouse client -n <<-EOSQL
	CREATE DATABASE IF NOT EXISTS dwh;
	CREATE TABLE IF NOT EXISTS dwh.exchange_rates_raw (
        base FixedString(3) NOT NULL, 
        code FixedString(3) NOT NULL, 
        date Date NOT NULL,
        rate float NULL,
        __dag_id__ String NOT NULL,
        __dag_run_id__ String NOT NULL,
        __dag_run_start_date__ datetime NOT NULL
        )
        ENGINE = Log;
    CREATE TABLE IF NOT EXISTS dwh.exchange_rates(
        base FixedString(3) NOT NULL, 
        code FixedString(3) NOT NULL, 
        date Date NOT NULL,
        rate float NULL,
        __last_update__ datetime NOT NULL
        )
        ENGINE = ReplacingMergeTree(__last_update__)
        ORDER BY (base, code, date);
		EOSQL
EOF

# "deploy" plugins and dags
cp -R src/plugins/* ./plugins/
cp -R src/dags/* ./dags/