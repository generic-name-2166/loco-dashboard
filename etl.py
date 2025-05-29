from prefect import flow, task


@task
def uhh_task() -> None:
    print("side effecting very hard")


@flow(log_prints=True)
def main() -> None:
    uhh_task()
    print("Hello from prefect-superset!")


if __name__ == "__main__":
    main.serve(
        name="first-prefect-deployment",
        cron="* * * * *",
        tags=["testing"],
        # kill the deployment on shutdown
        pause_on_shutdown=False,
    )
