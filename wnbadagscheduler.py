from prefect import flow
from main import run_wnba

@flow
def my_scheduled_flow():
    run_wnba()

if __name__ == "__main__":
    my_scheduled_flow.deploy(
        name="morning-run",
        schedule={"cron": "0 9 * * *", "timezone": "America/Los_Angeles"},
        work_pool_name="local-pool",
        path="."  # 👈 Required to run locally
    )

    my_scheduled_flow.deploy(
        name="afternoon-30min",
        schedule={"cron": "*/30 14-15 * * *", "timezone": "America/Los_Angeles"},
        work_pool_name="local-pool",
        path="."  # 👈 Required to run locally
    )
