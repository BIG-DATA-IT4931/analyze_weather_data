import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/strong-ward-437213-j6-c3ae16d10e5f.json"
project_id = "your_project_id"
bucket_name = 'your_bucket_name'
table_name = "your_object_name"
root_path=f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):
    if data is None:
        raise ValueError('Data is missing!')
    
    if not pd.api.types.is_datetime64_any_dtype(data['time']):
        data['time'] = pd.to_datetime(data['time'])

    data["year"] = data["time"].dt.year
    data["month"] = data["time"].dt.month
    data["day"] = data["time"].dt.day

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["year", "month", "day"],
        filesystem=gcs
    )
