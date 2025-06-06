import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from etl import DATE_COLUMNS


def main() -> None:
    con = sqlite3.connect("loco.sqlite")
    df = pd.read_sql("SELECT * FROM loco", con)  # pyright:ignore[reportUnknownMemberType]

    df["ctime"] = pd.to_datetime(df["ctime"], format="ISO8601")  # pyright:ignore[reportUnknownMemberType]
    for date_column in DATE_COLUMNS:
        df[date_column] = pd.to_datetime(df[date_column])  # pyright:ignore[reportUnknownMemberType]

    print(df.head())
    print(df["id"].nunique())

    semantics = df[(df["prs_loc"].isnull()) & (~df["ser_loc"].isnull())]
    print(semantics.head())

    _ = sns.scatterplot(df[["ctime", "prs_loc"]], x="ctime", y="prs_loc")
    plt.show()  # pyright:ignore[reportUnknownMemberType]

    _ = sns.lineplot(df.resample("1W", on="ctime").agg({"id": "count"}))  # pyright:ignore[reportUnknownMemberType]
    plt.show()  # pyright:ignore[reportUnknownMemberType]


if __name__ == "__main__":
    main()
