import sqlite3

import pandas as pd

from etl import DATE_COLUMNS


def main() -> None:
    con = sqlite3.connect("loco.sqlite")
    df = pd.read_sql("SELECT * FROM loco", con)  # pyright:ignore[reportUnknownMemberType]

    df["ctime"] = pd.to_datetime(df["ctime"], format="ISO8601")  # pyright:ignore[reportUnknownMemberType]
    for date_column in DATE_COLUMNS:
        df[date_column] = pd.to_datetime(df[date_column])  # pyright:ignore[reportUnknownMemberType]

    print(df.head())


if __name__ == "__main__":
    main()
