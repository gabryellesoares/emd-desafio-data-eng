from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect.storage import Local

from pipelines.schedules import minute_to_minute
from pipelines.tasks import (
    get_data,
    transform_data,
    generate_csv,
)

with Flow('BRT: Vehicles') as flow:
    data = get_data()
    transformed_data = transform_data(data)
    generate_csv(data)

flow.storage = Local('desafio_data_eng')
flow.executor = LocalDaskExecutor()
flow.schedule = minute_to_minute()