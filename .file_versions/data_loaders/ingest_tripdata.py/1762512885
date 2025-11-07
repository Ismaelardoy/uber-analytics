if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import sys
sys.path.append('src')
from etl.ingest_tripdata import ingest_tripdata

@data_loader
def load_data(*args, **kwargs):
    """
    Load trip data and store it as a temporary Parquet file or in-memory table.
    Returns the path or table name instead of the DataFrame itself.
    """
    df = ingest_tripdata()

    path = '/tmp/tripdata.parquet'
    df.write.mode('overwrite').parquet(path)

    return {'data_path': path}

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert 'data_path' in output, 'Expected key data_path missing'