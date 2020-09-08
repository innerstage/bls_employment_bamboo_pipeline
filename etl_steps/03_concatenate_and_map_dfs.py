import os
import pandas as pd
import csv
from util import STATE_NAMES, SUPERSECTOR_NAMES, INDUSTRY_NAMES


def concatenate_and_map():
    print("Concatenating, mapping and filtering...")
    filenames = [f for f in os.listdir("etl_steps/data_temp/download_series_by_state") if ".csv" in f]
    df_list = []

    for filename in filenames:
        df = pd.read_csv("etl_steps/data_temp/download_series_by_state/"+filename, names=["series_id", "year", "month", "value"])
        df_list.append(df)

    df = pd.concat(df_list)

    df["seasonal_adjustment"] = df["series_id"].str[2:3].map({"S": 1, "U": 0})
    df["state_code"] = df["series_id"].str[3:5]
    df["state_name"] = df["series_id"].str[3:5].map(STATE_NAMES)
    df["supersector_code"] = df["series_id"].str[10:12]
    df["supersector_name"] = df["series_id"].str[10:12].map(SUPERSECTOR_NAMES)
    df["industry_code"] = df["series_id"].str[10:18]
    df["industry_name"] = df["series_id"].str[10:18].map(INDUSTRY_NAMES)

    df = df.dropna()
    df = df[["series_id", "year", "month", "seasonal_adjustment", "state_code", "state_name", "supersector_code", "supersector_name", "industry_code", "industry_name", "value"]]
    df = df.rename(columns={"value": "employees"})
    #df.to_csv("etl_steps/data_output/single_output.csv", quoting=csv.QUOTE_NONNUMERIC, index=False)
    
    return df


def create_sadj_df(df, sadj):
    print(sadj.upper() + " Fact Table Processing...")

    df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
    df["state_id"] = df["state_code"].astype(int)
    df["industry_id"] = df["industry_code"].astype(int)

    df = df.rename(columns={"seasonal_adjustment": "seasonal_adjustment_id"})

    df = df[["time_id", "seasonal_adjustment_id", "state_id", "industry_id", "employees"]]
    df["employees"] = df["employees"] * 1000

    sadj_dict = {"sa": 1, "nsa": 0}

    df = df[df["seasonal_adjustment_id"]==sadj_dict[sadj]]
    df = df[["time_id", "state_id", "industry_id", "employees"]]
    df["time_id"] = df["time_id"].astype(int)
    df.to_csv("etl_steps/data_temp/base_" + sadj + "_fact.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

    print(sadj.upper() + " DataFrame created and saved!")

    return df


if __name__ == "__main__":
    df = concatenate_and_map()
    df_nsa = create_sadj_df(df, "nsa")
    df_sa = create_sadj_df(df, "sa")