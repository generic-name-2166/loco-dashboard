import pandas as pd


def main() -> None:
    df = pd.read_csv("data/0_loco_19.tsv", delimiter="\t")
    """ columns = {
        "loco_19.ctime",
        "loco_19.dan_type",
        "loco_19.ser_loc",
        "loco_19.zns_loc",
        "loco_19.prs_loc",
        "loco_19.repair_kind",
        "loco_19.kod_firm_repair",
        "loco_19.p_vyh_tch",
        "loco_19.p_prib_zav",
        "loco_19.p_tu162",
        "loco_19.p_zast_pot",
        "loco_19.p_demont",
        "loco_19.p_moika",
        "loco_19.p_kp",
        "loco_19.p_ted",
        "loco_19.p_tel_sbor",
        "loco_19.p_op_kuz",
        "loco_19.p_dgu_per",
        "loco_19.p_per_agr",
        "loco_19.p_el_mont",
        "loco_19.p_mzh_agr",
        "loco_19.p_sbor",
        "loco_19.p_vor_isp",
        "loco_19.p_vor_mor",
        "loco_19.p_vor_5",
        "loco_19.p_obkat",
        "loco_19.p_cta",
        "loco_19.p_otp_tch",
        "loco_19.p_ekspl",
        "loco_19.vyh_tch",
        "loco_19.prib_zav",
        "loco_19.tu162",
        "loco_19.zast_pot",
        "loco_19.demont",
        "loco_19.moika",
        "loco_19.kp",
        "loco_19.ted",
        "loco_19.tel_sbor",
        "loco_19.op_kuz",
        "loco_19.dgu_per",
        "loco_19.per_agr",
        "loco_19.el_mont",
        "loco_19.mzh_agr",
        "loco_19.sbor",
        "loco_19.vor_isp",
        "loco_19.vor_mor",
        "loco_19.vor_5",
        "loco_19.obkat",
        "loco_19.cta",
        "loco_19.otp_tch",
        "loco_19.ekspl",
    } """
    date_columns = {
        "loco_19.p_vyh_tch",
        "loco_19.p_prib_zav",
        "loco_19.p_tu162",
        "loco_19.p_zast_pot",
        "loco_19.p_demont",
        "loco_19.p_moika",
        "loco_19.p_kp",
        "loco_19.p_ted",
        "loco_19.p_tel_sbor",
        "loco_19.p_op_kuz",
        "loco_19.p_dgu_per",
        # ??
        "loco_19.p_el_mont",
        "loco_19.p_sbor",
        "loco_19.p_vor_isp",
        "loco_19.p_vor_mor",
        "loco_19.p_vor_5",
        "loco_19.p_obkat",
        "loco_19.p_cta",
        "loco_19.p_otp_tch",
        "loco_19.p_ekspl",
        "loco_19.vyh_tch",
        "loco_19.prib_zav",
        "loco_19.tu162",
        "loco_19.zast_pot",
        "loco_19.demont",
        "loco_19.moika",
        "loco_19.kp",
        "loco_19.ted",
        "loco_19.tel_sbor",
        "loco_19.op_kuz",
        "loco_19.dgu_per",
        # ??
        "loco_19.el_mont",
        "loco_19.mzh_agr",
        "loco_19.sbor",
        "loco_19.vor_isp",
        "loco_19.vor_mor",
        "loco_19.vor_5",
        "loco_19.obkat",
        "loco_19.cta",
        "loco_19.otp_tch",
        "loco_19.ekspl",
    }
    print(df.iat[1, 49])
    df["loco_19.ctime"] = pd.to_datetime(df["loco_19.ctime"], format="%Y-%m-%d %H:%M:%S.%f")
    for date_column in date_columns:
        df[date_column] = pd.to_datetime(df[date_column])
    print(df.iat[1, 49])
    # df = pd.read_csv("data/1_loco_19.tsv", delimiter="\t")
    # df.info()


if __name__ == "__main__":
    main()
