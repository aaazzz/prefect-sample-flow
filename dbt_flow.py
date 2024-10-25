import os
from prefect import flow, task
from prefect_dbt import DbtCoreOperation

@task
def run_dbt():
    dbt_op = DbtCoreOperation.load("dbt-code-operation")
    result = dbt_op.run()
    return result

@flow
def my_flow():
    run_dbt()


if __name__ == "__main__":
    my_flow.serve(
        name="run-dbt-on-local",
        interval=60
     )
