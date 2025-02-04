import datetime

def get_hive_partition_prefix_str(partition_time: datetime):
    return partition_time.strftime("dt=%Y-%m-%d")

def get_execution_date(**kwargs):
    return kwargs['execution_date']