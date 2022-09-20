FROM clickhouse/clickhouse-server 
ADD  ./init.dwh /docker-entrypoint-initdb.d