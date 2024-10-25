import os
from prefect import flow, task
from prefect_dbt import DbtCoreOperation

@task
def run_dbt(use_block: bool = False):
    if use_block:
        dbt_op = DbtCoreOperation.load("dbt-code-operation")
        result = dbt_op.run()

    else:
        # ローカル実行時
        dbt_op = DbtCoreOperation(
            commands=["pwd", "dbt run"],
            project_dir="./flow/dbt/dbt_project",
            profiles_dir="./flow/dbt"
        )
        result = dbt_op.run()
    return result


@flow
def my_flow(use_block: bool = False):
    run_dbt(use_block=use_block)


if __name__ == "__main__":
    my_flow.serve(
        name="run-dbt-on-local",
        interval=60,
        parameters={"use_block": False}
     )
