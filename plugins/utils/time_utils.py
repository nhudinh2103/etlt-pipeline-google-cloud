import datetime
from airflow.utils.dates import parse_execution_date

def get_hive_partition_prefix_str(partition_time: datetime):
    return partition_time.strftime("dt=%Y-%m-%d")

def get_execution_date_as_datetime(execution_date: str) -> datetime:
    """
    Converts execution_date from a Jinja template string to a datetime object.
    
    :param execution_date: Execution date string from Airflow (templated).
    :return: A datetime object.
    """
    if isinstance(execution_date, datetime):
        return execution_date  # Already a datetime object
    
    return parse_execution_date(execution_date)  # Convert string to datetime