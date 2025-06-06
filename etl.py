import os
from pathlib import Path
import sqlite3
from sqlite3 import Connection

import pandas as pd
from prefect import flow, task
from prefect.cache_policies import NO_CACHE


CREATE_COMPLETED: str = """
CREATE TABLE IF NOT EXISTS completed (filename TEXT)
"""
CREATE_LOCO: str = """
CREATE TABLE IF NOT EXISTS loco (
    ctime,
    dan_type,
    ser_loc,
    zns_loc,
    prs_loc,
    repair_kind,
    kod_firm_repair,
    p_vyh_tch,
    p_prib_zav,
    p_tu162,
    p_zast_pot,
    p_demont,
    p_moika,
    p_kp,
    p_ted,
    p_tel_sbor,
    p_op_kuz,
    p_dgu_per,
    p_per_agr,
    p_el_mont,
    p_mzh_agr,
    p_sbor,
    p_vor_isp,
    p_vor_mor,
    p_vor_5,
    p_obkat,
    p_cta,
    p_otp_tch,
    p_ekspl,
    vyh_tch,
    prib_zav,
    tu162,
    zast_pot,
    demont,
    moika,
    kp,
    ted,
    tel_sbor,
    op_kuz,
    dgu_per,
    per_agr,
    el_mont,
    mzh_agr,
    sbor,
    vor_isp,
    vor_mor,
    vor_5,
    obkat,
    cta,
    otp_tch,
    ekspl
)
"""


@task(cache_policy=NO_CACHE)
def check_completed(conn: Connection) -> pd.DataFrame:
    cur = conn.cursor()
    _ = cur.execute(CREATE_COMPLETED)
    _ = cur.execute(CREATE_LOCO)
    conn.commit()

    return pd.read_sql("SELECT * FROM completed", conn)  # pyright:ignore[reportUnknownMemberType]


@task(cache_policy=NO_CACHE)
def list_data(path: Path) -> set[Path]:
    filenames = next(os.walk(path), (None, None, []))[2]  # pyright:ignore[reportUnknownArgumentType]
    return set(
        map(lambda x: path.joinpath(x), filter(lambda x: x.endswith(".tsv"), filenames))
    )


COLUMNS = (
    "ctime",
    "dan_type",
    "ser_loc",
    "zns_loc",
    "prs_loc",
    "repair_kind",
    "kod_firm_repair",
    "p_vyh_tch",
    "p_prib_zav",
    "p_tu162",
    "p_zast_pot",
    "p_demont",
    "p_moika",
    "p_kp",
    "p_ted",
    "p_tel_sbor",
    "p_op_kuz",
    "p_dgu_per",
    "p_per_agr",
    "p_el_mont",
    "p_mzh_agr",
    "p_sbor",
    "p_vor_isp",
    "p_vor_mor",
    "p_vor_5",
    "p_obkat",
    "p_cta",
    "p_otp_tch",
    "p_ekspl",
    "vyh_tch",
    "prib_zav",
    "tu162",
    "zast_pot",
    "demont",
    "moika",
    "kp",
    "ted",
    "tel_sbor",
    "op_kuz",
    "dgu_per",
    "per_agr",
    "el_mont",
    "mzh_agr",
    "sbor",
    "vor_isp",
    "vor_mor",
    "vor_5",
    "obkat",
    "cta",
    "otp_tch",
    "ekspl",
)
DATE_COLUMNS = {
    "p_vyh_tch",
    "p_prib_zav",
    "p_tu162",
    "p_zast_pot",
    "p_demont",
    "p_moika",
    "p_kp",
    "p_ted",
    "p_tel_sbor",
    "p_op_kuz",
    "p_dgu_per",
    # ??
    "p_el_mont",
    "p_sbor",
    "p_vor_isp",
    "p_vor_mor",
    "p_vor_5",
    "p_obkat",
    "p_cta",
    "p_otp_tch",
    "p_ekspl",
    "vyh_tch",
    "prib_zav",
    "tu162",
    "zast_pot",
    "demont",
    "moika",
    "kp",
    "ted",
    "tel_sbor",
    "op_kuz",
    "dgu_per",
    # ??
    "el_mont",
    "mzh_agr",
    "sbor",
    "vor_isp",
    "vor_mor",
    "vor_5",
    "obkat",
    "cta",
    "otp_tch",
    "ekspl",
}


def read_tsv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", names=COLUMNS)  # pyright:ignore[reportUnknownMemberType]
    df["ctime"] = pd.to_datetime(  # pyright:ignore[reportUnknownMemberType]
        df["ctime"], format="%Y-%m-%d %H:%M:%S.%f"
    )
    for date_column in DATE_COLUMNS:
        df[date_column] = pd.to_datetime(df[date_column])  # pyright:ignore[reportUnknownMemberType]

    df["id"] = df.groupby(["ser_loc", "zns_loc", "prs_loc"]).ngroup()  # pyright:ignore[reportUnknownMemberType]
    return df


@task(cache_policy=NO_CACHE)
def load_data(con: Connection, path: Path) -> None:
    df = read_tsv(path)
    _: int | None = df.to_sql("loco", con, if_exists="replace", index=False)


@task(cache_policy=NO_CACHE)
def mark_completed(con: Connection, paths: set[Path]) -> None:
    filenames = map(str, paths)
    df = pd.DataFrame(filenames, columns=["filename"])
    _: int | None = df.to_sql("completed", con, if_exists="append", index=False)


@flow
def main() -> None:
    conn: Connection = sqlite3.connect("loco.sqlite")
    completed: set[Path] = set(
        check_completed(conn)["filename"].map(Path, na_action="ignore")  # pyright:ignore[reportUnknownMemberType]
    )
    filenames: set[Path] = list_data(Path("data"))
    to_load: set[Path] = filenames - completed
    for path in to_load:
        load_data(conn, path)
    mark_completed(conn, to_load)
    conn.close()


if __name__ == "__main__":
    main.serve(
        name="first-prefect-deployment",
        cron="* * * * *",
        tags=["testing"],
        # kill the deployment on shutdown
        pause_on_shutdown=False,
    )
