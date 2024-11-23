from prefect import flow
from typing import Optional


@flow(log_prints=True)
def my_flow(date: Optional[datetime] = None):
    if date is None:
        date = datetime.now(timezone.utc)
    print(f"It was {date.strftime('%A')} on {date.isoformat()}")


if __name__ == "__main__":
    my_flow.serve(
        name="run-on-local",
        interval=60
    )
