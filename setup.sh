#!/bin/bash

# Exercise environment preset for docker-compose.yaml.old

set -e

# flush previous setups if exists
rm -Rf ./dags ./logs ./plugins ./init.dwh ./keys
rm -f .env

# setup docker-compose volumes
mkdir ./logs ./init.dwh
ln -s "/Users/patraden/PycharmProjects/airflow/dags" .
ln -s "/Users/patraden/PycharmProjects/airflow/plugins" .
ln -s "/Users/patraden/PycharmProjects/airflow/keys" .

# setup airflow environment variables
echo "AIRFLOW_UID=$(id -u)" > .env
echo "CONN_DWH='{\"conn_type\": \"sqlite\", \"host\": \"dwh\", \"schema\": \"dwh\", \"extra\": \"\"}'" >> .env
echo "CONN_DATABRICKS='{\"conn_type\": \"databricks\",\"login\": \"token\",\"password\": \"$(echo $TOKEN)\",\"host\": \"https://2069279851173120.0.gcp.databricks.com\"}'" >> .env
echo "CONN_API_PROXY_FBS_EU='{\"conn_type\": \"http\",\"host\": \"http://api-proxy.fbs.eu\"}'" >> .env
echo "CONN_API_PROXY_MY_FBS_COM='{\"conn_type\": \"http\",\"host\": \"http://api-proxy.my.fbs.com\"}'" >> .env

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
