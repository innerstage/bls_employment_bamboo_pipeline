import pandas as pd

old = pd.read_csv("old_industries.csv")
new = pd.read_csv("industry_dimension.csv")

old_sup = sorted(list(old["supersector_name"].unique()))
new_sup = sorted(list(new["industry_name"].unique()))

for (o,n) in zip(old_sup, new_sup):
    print(o, "|", n)