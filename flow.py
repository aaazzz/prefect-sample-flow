from prefect import flow
import duckdb

@flow(log_prints=True)
def my_flow():
    df = duckdb.sql("SELECT 42")
    print(df)


if __name__ == "__main__":
    my_flow.serve(
        name="run-on-local",
        interval=60
    )
