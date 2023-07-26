from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect.storage import Local

from pipelines.schedules import minute_to_minute
from pipelines.tasks import (
    get_data,
    process_data,
    generate_csv,
    store_data,
)

with Flow('BRT: Vehicles', schedule=minute_to_minute) as flow:
    data = get_data()
    df = process_data(data)
    final_df = generate_csv(df)
    store_data(final_df)

# flow.storage = Local('desafio_data_eng')
flow.executor = LocalDaskExecutor()
# flow.schedule = minute_to_minute()