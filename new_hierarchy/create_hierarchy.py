import pandas as pd
import csv


df = pd.read_csv("final_industries.csv", dtype="object")

def code_reader(c):
    if c[6:] != "00":
        return 10
    elif c[1:] == "0000000":
        return 0
    elif c[2:] == "000000":
        return 2
    elif c[4:] == "0000":
        return 4
    elif c[5:] == "000":
        return 6
    elif c[6:] == "00":
        return 8

df["level"] = pd.Series([code_reader(c) for c in df["code"]])

print(df)

row_list = [[""]*12 for l in range(len(df))]

for i,r in df.iterrows():
    row_list[i][r["level"]] = r["code"]
    row_list[i][r["level"]+1] = r["name"]

df = pd.DataFrame(columns=["L1_code", "L1_name", "L2_code", "L2_name", "L3_code", "L3_name", "L4_code", "L4_name", "L5_code", "L5_name", "L6_code", "L6_name"], data=row_list)

df.to_csv("industry_levels.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
