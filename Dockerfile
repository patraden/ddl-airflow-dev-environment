FROM clickhouse/clickhouse-server 
ADD  ./clickhouse-entrypoint-initdb.d /docker-entrypoint-initdb.d