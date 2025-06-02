import sqlite3

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
def check_completed(conn: sqlite3.Connection) -> pd.DataFrame:
    cur = conn.cursor()
    _ = cur.execute(CREATE_COMPLETED)
    _ = cur.execute(CREATE_LOCO)
    conn.commit()

    return pd.read_sql("SELECT * FROM completed", conn)  # pyright:ignore[reportUnknownMemberType]


@flow
def main() -> None:
    conn = sqlite3.connect("loco.sqlite")
    completed = check_completed(conn)
    print(completed, type(completed))
    conn.close()


if __name__ == "__main__":
    main.serve(
        name="first-prefect-deployment",
        cron="* * * * *",
        tags=["testing"],
        # kill the deployment on shutdown
        pause_on_shutdown=False,
    )
