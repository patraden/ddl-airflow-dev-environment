import csv
from clickhouse_driver import Client
from pathlib import Path
from datetime import datetime, date
from airflow.models import Variable
from typing import Iterable, Any, Type, List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.taskinstance import TaskInstance

# Assuming get method gets values from env rather than metadata db
BASE, CODE = Variable.get("EXCHANGERATE_HOST_PAIR", deserialize_json=True).values()
STORAGE_MOUNT_POINT = Variable.get("FILE_STORAGE_MOUNT_POINT") 
HISTORY_START_DATE = datetime.strptime(Variable.get("EXCHANGERATE_HOST_HISTORY_START"), '%Y-%m-%d').date()
HISTORY_LOAD = bool(Variable.get("EXCHANGERATE_HOST_HISTORY_LOAD"))
DWH_HOST_NAME = "dwh"

PRECISION = 6
DWH_RAW_TABLE = "dwh.exchange_rates_raw"
DWH_RAW_TABLE_SCHEMA = set([
    "base",
    "code",
    "date",
    "rate",
    "__dag_id__",
    "__dag_run_id__",
    "__dag_run_start_date__"
    ])

def save_data_as_csv(
    header: Iterable[Any], 
    rows: Iterable[Iterable[Any]], 
    filepath: Path
    ) -> None:
    """ 
    Serialize python data into csv. 
    """
    with open(str(filepath), "w") as file:
        csv_writer = csv.writer(file,  doublequote=True)
        csv_writer.writerow(header)
        csv_writer.writerows(rows)
        file.flush()

def postgres_dql_to_csv(sql: str, filename: str) -> None:
    """ 
    Leverage postgres hook (wrapper of psycopg2) to save query result as csv 
    """
    hook = PostgresHook(postgres_conn_id="test_db")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    save_data_as_csv(header=[i[0] for i in cursor.description], rows=cursor, filepath=Path(STORAGE_MOUNT_POINT) / filename)
    cursor.close()
    conn.close()

def load_text_file(path: str) -> str:
    """ 
    Reads text file 
    """
    return open(path, 'r', encoding = "utf8").read()

def csv_format_response_timeseries_insert_to_clickhouse(response, ti):
    """
    https://exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}&base={cur_base}&symbols=USD&format=csv
    """
    reader = list(filter(lambda line: line != [], csv.reader(response.text.split("\n"))))
    header, data = reader[0], reader[1:]
    data_dict = list(map(lambda row: dict(zip(header, row)), data))
    for row in data_dict:
        row.pop("start_date")
        row.pop("end_date")
        row["date"] = date.fromisoformat(row["date"])
        row["rate"] = float(row["rate"].replace(",", "."))
        row["__dag_id__"] = f"{ti.dag_run.dag_id}"
        row["__dag_run_id__"] = f"{ti.dag_run.id}"
        row["__dag_run_start_date__"] = datetime.fromisoformat(f"{ti.dag_run.start_date}")
    assert(DWH_RAW_TABLE_SCHEMA == set(data_dict[0]), "Uoops, got an unexpected timeseries api response schema mismatch!")
    
    sql = f"INSERT INTO {DWH_RAW_TABLE} ({','.join(c for c in data_dict[0])}) VALUES"
    client = Client(host=DWH_HOST_NAME)
    return client.execute(query=sql, params=data_dict)

def history_split_by_year(
    fr: Type[date], 
    to: Type[date]
    ) -> List[Tuple[str]]:
    """ 
    Split hitorical interval (from, to) into calendar year intervals.
    Intervals returned as iso formatted string tuples. 
    """
    assert ((to - fr).days > -1), f"start [{fr}] is later than end [{to}]"
    ranges = [
        (f"{fr.year + year_delta}-01-01", f"{fr.year + year_delta}-12-31") 
        for year_delta in range(to.year - fr.year + 1)
        ]
    ranges[0] = (fr.isoformat(), ranges[0][1])
    ranges[-1] = (ranges[-1][0], to.isoformat())
    return ranges