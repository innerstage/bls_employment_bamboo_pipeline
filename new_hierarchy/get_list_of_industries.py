import pandas as pd
import numpy as np
import csv


df1 = pd.read_csv("data_output/bls_nsa_fact.csv")
df2 = pd.read_csv("data_output/bls_sa_fact.csv")

nsa_array = df1["industry_id"].astype(str).str.zfill(8).unique()
sa_array = df2["industry_id"].astype(str).str.zfill(8).unique()
total_array = np.unique(np.concatenate([nsa_array, sa_array]))

dfn = pd.read_csv("https://download.bls.gov/pub/time.series/sm/sm.industry", sep="\t")
dfn["industry_code"] = dfn["industry_code"].astype(str).str.zfill(8)
name_map = {code:name for (code,name) in zip(dfn["industry_code"], dfn["industry_name"])}

df = pd.DataFrame({"code": total_array})
df["name"] = df["code"].map(name_map)
df.to_csv("dimension_reduction/final_industries.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)