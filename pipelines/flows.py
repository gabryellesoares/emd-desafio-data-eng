from prefect import Flow
from pipelines.schedules import minute_to_minute
from pipelines.tasks import (
    get_data,
    process_data,
    generate_csv,
    store_data,
)

with Flow('BRT: Vehicles', schedule=minute_to_minute) as flow:
    data = get_data()
    df_brt = process_data(data)
    csv_file = generate_csv(df_brt)
    store_data(csv_file)
