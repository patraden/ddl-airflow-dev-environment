import csv
from copy import deepcopy
from pathlib import Path
from datetime import datetime, date
from airflow.models import Variable
from typing import Iterable, Any, Type, List, Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.models.taskinstance import TaskInstance

# Assuming get method gets values from env rather than metadata db (slow?)
BASE, CODE = Variable.get("EXCHANGERATE_HOST_PAIR", deserialize_json=True).values()
STORAGE_MOUNT_POINT = Variable.get("FILE_STORAGE_MOUNT_POINT") 
HISTORY_START_DATE = datetime.strptime(Variable.get("EXCHANGERATE_HOST_HISTORY_START"), '%Y-%m-%d').date()
HISTORY_LOAD = bool(Variable.get("EXCHANGERATE_HOST_HISTORY_LOAD"))
PRECISION = 6
CSV_RESPONSE_SCHEMA = ["base","code","date","rate"]
DWH_RAW_TABLE = "dwh.exchange_rates_raw"
DWH_TABLE = "dwh.exchange_rates"
DWH_RAW_TABLE_SCHEMA = CSV_RESPONSE_SCHEMA + ["__dag_id__","__dag_run_id__","__dag_run_start_date__"]
DWH_TABLE_SCHEMA = CSV_RESPONSE_SCHEMA + ["__last_update__"]

def read_text_file(path: str) -> str:
    """ The simpliest text file reader """
    return open(path, 'r', encoding = "utf8").read()

def _save_data_as_csv(
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
    Leverage postgres hook (wrapper of psycopg2) to save query result as csv. 
    """
    hook = PostgresHook(postgres_conn_id="test_db")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    _save_data_as_csv(header=[i[0] for i in cursor.description], rows=cursor, filepath=Path(STORAGE_MOUNT_POINT) / filename)
    cursor.close()
    conn.close()

def csv_response_insert_into_dwh_raw(response, ti: TaskInstance):
    """
    Insert csv formatted response into dwh clickhouse db to raw table.Accepts endpoints:
    https://exchangerate.host/latest
    https://exchangerate.host/timeseries
    """
    data = _csv_response_prepare(response)
    data_extended = _extend_to_raw(data, ti)
    cols = ','.join(c for c in data_extended[0])
    return _insert_into_dwh(data_extended, table=DWH_RAW_TABLE, columns=cols)

def csv_response_insert_into_dwh(response, ti: TaskInstance):
    """
    Insert csv formatted response into dwh clickhouse db to both raw and optimized tables.Accepts endpoints:
    https://exchangerate.host/latest
    https://exchangerate.host/timeseries
    """
    data = _csv_response_prepare(response)
    data_raw = _extend_to_raw(data, ti)
    data_optimized = _extend_to_optimized(data, ti)
    cols_raw = ','.join(c for c in data_raw[0])
    cols_optimized = ','.join(c for c in data_optimized[0])
    res_raw = _insert_into_dwh(data_raw, table=DWH_RAW_TABLE, columns=cols_raw)
    res_optimized = _insert_into_dwh(data_optimized, table=DWH_TABLE, columns=cols_optimized)
    return res_raw, res_optimized

def _csv_response_prepare(response) -> List[Any]:
    """
    Validates and converts csv string into python list of rows.
    """
    reader = list(filter(lambda line: line != [], csv.reader(response.text.split("\n"))))
    header, data = reader[0], reader[1:]
    rows = list(map(lambda row: dict(zip(header, row)), data))
    for row in rows:
        if "start_date" in row or "end_date" in row:
            row.pop("start_date")
            row.pop("end_date")
        row["date"] = date.fromisoformat(row["date"])
        row["rate"] = float(row["rate"].replace(",", "."))
    assert(set(CSV_RESPONSE_SCHEMA) == set(rows[0]), f"Uoops, got an unexpected api response schema {rows[0]}!")
    return rows

def _extend_to_raw(
    data: List[Any], 
    ti: TaskInstance) -> List[Any]:
    """
    Extends data list with additional technical columns inline with raw table schema.
    """
    data_copy = deepcopy(data)
    for row in data_copy:
        row["__dag_id__"] = f"{ti.dag_run.dag_id}"
        row["__dag_run_id__"] = f"{ti.dag_run.id}"
        row["__dag_run_start_date__"] = datetime.fromisoformat(f"{ti.dag_run.start_date}")
    return data_copy

def _extend_to_optimized(
    data: List[Any], 
    ti: TaskInstance) -> List[Any]:
    """
    Extends data list with additional technical columns inline with optimized table schema.
    """
    data_copy = deepcopy(data)
    for row in data_copy:
        row["__last_update__"] = datetime.fromisoformat(f"{ti.dag_run.start_date}")
    return data_copy

def _insert_into_dwh(
    data: List[Any], 
    table: str, 
    columns: str) -> Any:
    """
    Data insert into "dwh" clickhouse db.
    """
    sql = f"INSERT INTO {table} ({columns}) VALUES"
    hook = ClickHouseHook(clickhouse_conn_id="dwh")
    return hook.run(sql=sql, parameters=data)

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