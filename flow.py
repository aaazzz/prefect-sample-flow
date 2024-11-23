from prefect import flow

@flow(log_prints=True)
def my_flow():
    print('hello!')


if __name__ == "__main__":
    my_flow.serve(
        name="run-on-local",
        interval=60
    )
