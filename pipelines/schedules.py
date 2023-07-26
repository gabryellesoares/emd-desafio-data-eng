from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from datetime import datetime, timedelta
import pytz

current_time = datetime.now(pytz.timezone('America/Sao_Paulo'))

minute_to_minute = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=current_time + timedelta(seconds=5),
            end_date=current_time + timedelta(minutes=10, seconds=5),
        )
    ]
)