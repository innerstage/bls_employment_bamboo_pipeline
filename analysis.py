import pandas as pd
import numpy as np
from util import VALID_SERIES_BY_STATE, STATE_NAMES, INDUSTRY_NAMES, SUPERSECTOR_NAMES


def analyze_state(d):
    code = d["code"]
    name = d["name"].replace(" ","_")
    series_in_dict = d["series"]

    temp_filename = "data_temp/" + code + "_" + name + ".csv"
    temp_df = pd.read_csv(temp_filename, names=["series_id", "year", "month", "value"])
    temp_df["adj"] = temp_df["series_id"].str[2]
    temp_nsa_rows = len(temp_df[temp_df["adj"]=="U"])
    temp_sa_rows = len(temp_df[temp_df["adj"]=="S"])

    sa_df = pd.read_csv("data_output/bls_sa_fact.csv")
    final_sa_rows = len(sa_df[sa_df["state_id"]==int(code)])

    nsa_df = pd.read_csv("data_output/bls_nsa_fact.csv")
    final_nsa_rows = len(nsa_df[nsa_df["state_id"]==int(code)])

    series_in_file = list(temp_df["series_id"].unique())
    missing = [s for s in series_in_dict if s not in series_in_file] 

    print(code, name, ":")
    print("-- SA: temp = {} | final = {} | {}".format(temp_sa_rows, final_sa_rows, temp_sa_rows == final_sa_rows))
    print("-- NSA: temp = {} | final = {} | {}".format(temp_nsa_rows, final_nsa_rows, temp_nsa_rows == final_nsa_rows))
    print("-- Missing Series: ", missing)


for d in VALID_SERIES_BY_STATE:
    analyze_state(d)
