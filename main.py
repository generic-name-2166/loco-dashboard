from prefect import flow, task


@task
def uhh_task() -> None:
    print("side effecting very hard")


@flow
def main() -> None:
    uhh_task()
    print("Hello from prefect-superset!")


if __name__ == "__main__":
    main()
