from prefect import flow
from main import main


@flow
def wnba_pipeline():
    main()


if __name__ == "__main__":
    wnba_pipeline.deploy(
        name="morning-run",
        schedule={"cron": "0 9 * * *", "timezone": "America/Los_Angeles"},
        work_pool_name="local-pool",
        path=".",
    )

    wnba_pipeline.deploy(
        name="afternoon-30min",
        schedule={"cron": "*/30 14-15 * * *", "timezone": "America/Los_Angeles"},
        work_pool_name="local-pool",
        path=".",
    )
