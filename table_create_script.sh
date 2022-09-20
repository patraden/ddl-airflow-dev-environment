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