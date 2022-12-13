# all of a sudden this issues with gcc came up. Solved by:
# https://unix.stackexchange.com/questions/621462/cannot-execute-gcc-due-to-permission-denied

FROM apache/airflow:2.3.3-python3.8
COPY requirements.txt /
USER root
RUN apt-get update && sudo apt-get install -y --reinstall gcc
COPY id_rsa_airflow /opt/airflow/.ssh/id_rsa_airflow
RUN chown 50000:0 /opt/airflow/.ssh/id_rsa_airflow && chmod 400 /opt/airflow/.ssh/id_rsa_airflow
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt