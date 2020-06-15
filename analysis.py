import pandas as pd

df = pd.read_csv("data_output/single_output.csv")
df["time_id"] = df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
print(df["time_id"].max())