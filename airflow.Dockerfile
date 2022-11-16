# docker build --tag extended_airflow:latest --file "airflow.Dockerfile" .
FROM apache/airflow:2.3.3-python3.9
COPY requirements.txt /
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt