from clickhouse_driver import Client
import requests
from pathlib import Path
from datetime import date
import csv

# https://clickhouse-driver.readthedocs.io/en/latest/index.html

def get_data_api(path: Path = Path("/Users/patraden/IdeaProjects/homework/src/tests/data.csv")):
    url = 'https://api.exchangerate.host/timeseries?start_date=2021-01-01&end_date=2021-12-31&base=BTC&symbols=USD&format=CSV&places=6'
    response = requests.get(url)
    open(str(path), 'w', encoding = "utf8").write(response.text)

def get_data_file(path: Path = Path("/Users/patraden/IdeaProjects/homework/src/tests/data.csv")):
    DWH_RAW_TABLE_SCHEMA = "base,code,date,rate,__dag_id__,__dag_run_id__,__dag_run_start_date__"
    content = open(str(path), 'r', encoding = "utf8").read().split("\n")
    reader = list(csv.reader(content))
    header, data = reader[0], reader[1:]
    data_dict = list(map(lambda row: dict(zip(header, row)), data))
    for row in data_dict:
        row.pop("start_date")
        row.pop("end_date")
        row["date"] = date.fromisoformat(row["date"])
        row["rate"] = float(row["rate"].replace(",", "."))
    
    print(set(data_dict[0]))
    return header, data_dict

def insert_into_clickhouse(head, data):
   client = Client(host="localhost")
   sql = f"INSERT INTO dwh.exchange_rates (date, code, rate, base) VALUES"
   res = client.execute(query=sql, params=data)
   print(res)

if __name__ == "__main__":
    # get_data_api()
    get_data_file()
    # print(date.fromisoformat('2021-01-01'))
    # print(float('28990,047619'.replace(",", ".")))
    # insert_into_clickhouse(*get_data_file())