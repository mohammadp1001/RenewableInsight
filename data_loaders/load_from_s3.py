from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_s3_bucket(**kwargs):
    """
    Loading data from a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = kwargs['bucket_name'] 
    object_key = f"{kwargs['data_item_name']}_{kwargs['data_item_no']}_{kwargs['day']:02}_{kwargs['month']:02}_{kwargs['year']}/{kwargs['data_item_name']}_{kwargs['data_item_no']}.parquet"
    print(object_key)
    return S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )