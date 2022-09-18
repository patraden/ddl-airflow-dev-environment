import csv
from pathlib import Path
from datetime import datetime
from airflow.models import Variable
from typing import Iterable, Any

# Assuming get method gets values from env rather than metadata db
BASE, CODE = Variable.get("EXCHANGERATE_HOST_PAIR", deserialize_json=True).values()
STORAGE_MOUNT_POINT = Variable.get("FILE_STORAGE_MOUNT_POINT") 
HISTORY_START_DATE = datetime.strptime(Variable.get("EXCHANGERATE_HOST_HISTORY_START"), '%Y-%m-%d').date()
HISTORY_LOAD = bool(Variable.get("EXCHANGERATE_HOST_HISTORY_LOAD"))

DATASET_ID = f"{BASE}_{CODE}"
PRECISION = 6
DWH_TABLE = "exchange_rates"
DWH_TABLE_SCHEMA = '"base","code","date","rate","__dag_id__","__dag_run_id__","__dag_run_start_date__"'

def save_data_as_csv(
    header: Iterable[Any], 
    rows: Iterable[Iterable[Any]], 
    filepath: Path = Path(".") / "file.csv"
    ) -> None:
    """ Serialize python data into csv. """
    with open(str(filepath), "w") as file:
        csv_writer = csv.writer(file,  doublequote=True)
        csv_writer.writerow(header)
        csv_writer.writerows(rows)
        file.flush()