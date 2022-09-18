# homework
<<<<<<< Updated upstream
Task1
Task2
=======

# my custom clickhouse testing
```bash
docker build -t my-custom-clickhouse-image .
docker run -d --name my-clickhouse-server --ulimit nofile=262144:262144 my-custom-clickhouse-image
docker exec -it my-clickhouse-server clickhouse-client
```
>>>>>>> Stashed changes
