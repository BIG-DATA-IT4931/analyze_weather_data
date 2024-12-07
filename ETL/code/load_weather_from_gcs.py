import pyarrow as pa
from pyarrow import parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    bucket_name = 'your_bucket_name'
    folder_name = 'your_object_name'
    root_path = f'{bucket_name}/{folder_name}'

    gcs = pa.fs.GcsFileSystem()
    df = pq.ParquetDataset(root_path, filesystem=gcs)
        
    return df.read_pandas().to_pandas()



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
