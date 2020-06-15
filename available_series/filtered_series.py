import pandas as pd
import os
import csv
from util import STATE_NAMES


all_dfs = []
all_files = list(os.listdir("data_temp"))

for filename in all_files:
    df = pd.read_csv("data_temp/" + filename)
    df["State"] = df["Series ID"].str[3:5].map(STATE_NAMES)
    all_dfs.append(df)


df = pd.concat(all_dfs)
df = df[(df["Area"]=="Statewide") & (df["Datatype"]=="All Employees, In Thousands") & (df["Adjustment Method"]=="Not Seasonally Adjusted")]
df = df[["Series ID", "Industry", "State"]]
df.to_csv("NSA_series.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
