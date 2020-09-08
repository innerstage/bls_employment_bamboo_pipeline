import pandas as pd
import csv
from calendar import month_name
from util import STATE_NAMES


def create_time_dimension():
    print("Creating Time Dimension...")

    sa_df = pd.read_csv("etl_steps/relational_model/bls_fact_sa.csv", usecols=["time_id"])
    min_date = str(sa_df["time_id"].min())[:4] + "-" + str(sa_df["time_id"].min())[4:]
    max_date = str(sa_df["time_id"].max())[:4] + "-" + str(sa_df["time_id"].max())[4:]

    df = pd.DataFrame({"date": pd.date_range(min_date, max_date, freq="M")})
    df["year"] = df.date.dt.year
    df["quarter"] = "Q" + df.date.dt.quarter.astype(str)
    df["month"] = df.date.dt.month
    df["month_name"] = df["month"].map({k:month_name[k] for k in range(1,13)})
    df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
    df["date_name"] = df["year"].astype(str) + " " + df["month_name"]

    df = df[["time_id", "date_name", "year", "quarter", "month", "month_name"]]

    df.to_csv("etl_steps/relational_model/bls_dim_time.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

    print("Time dimension ready!")

    return df


def create_state_dimension():
    print("Creating state dimension...")
    
    state_codes = pd.Series(list(STATE_NAMES.keys()))

    state_dict = {"state_id": state_codes.astype(int), "state_name": state_codes.map(STATE_NAMES)}
    
    df = pd.DataFrame(state_dict)
    df["fips_code"] = "04000US" + df["state_id"].astype(str).str.zfill(2)

    df.to_csv("etl_steps/relational_model/bls_dim_state.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

    print("State dimension ready!")
    
    return df


if __name__ == "__main__":
    create_time_dimension()
    create_state_dimension()