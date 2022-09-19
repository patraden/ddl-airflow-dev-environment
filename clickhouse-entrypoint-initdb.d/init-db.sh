#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
	create database dwh;
	create table if not exists dwh.exchange_rates_raw (
        base FixedString(3) NOT NULL, 
        code FixedString(3) NOT NULL, 
        date Date NOT NULL,
        rate float NULL,
        __dag_id__ String NOT NULL,
        __dag_run_id__ String NOT NULL,
        __dag_run_start_date__ datetime NOT NULL
        )
        engine = Log;
		EOSQL