import os
from prefect import flow, task
from prefect_dbt import DbtCoreOperation
from prefect.logging import get_run_logger


@task
def run_dbt(use_block: bool = False, project_dir: str = "", profiles_dir: str = ""):
    if use_block:
        dbt_op = DbtCoreOperation.load("dbt-code-operation")
        result = dbt_op.run()

    else:
        dbt_op = DbtCoreOperation(
            commands=["dbt run"], project_dir=project_dir, profiles_dir=profiles_dir
        )
        result = dbt_op.run()

    return result


@task
def get_pwd() -> str:
    logger = get_run_logger()
    current_dir = os.getcwd()
    logger.info("Current directory: " + current_dir)

    return current_dir


@flow
def my_flow(use_block: bool = False):
    current_dir = get_pwd()
    project_dir = current_dir + "/dbt/dbt_project"
    profiles_dir = current_dir + "/dbt"

    run_dbt(use_block=use_block, project_dir=project_dir, profiles_dir=profiles_dir)


if __name__ == "__main__":
    my_flow.serve(name="run-dbt-on-local", interval=10, parameters={"use_block": False})
