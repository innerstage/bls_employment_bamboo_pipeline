import pandas as pd
import json
from util import STATE_NAMES

df1 = pd.read_csv("SA_series.csv")
df2 = pd.read_csv("NSA_series.csv")

df = pd.concat([df1,df2]).drop(columns="Industry")
df["State Code"] = df["Series ID"].str[3:5]
df = df.sort_values(by="State Code", ignore_index=True)

valid_series = []
states = list(df["State Code"].unique())
for state in states:
    code = state
    name = STATE_NAMES[code]
    series = list(df[df["State Code"]==code]["Series ID"])
    valid_series.append({"code": code, "name": name, "series": series})

df_json = json.dumps(valid_series)
with open("valid_series.json", "w") as file:
    file.write(df_json)
    
print("Completed!")
