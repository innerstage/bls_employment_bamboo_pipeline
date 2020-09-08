import pandas as pd
import numpy as np
import csv
import json
import os


def init():
    if not os.path.isdir("etl_steps/relational_model"):
        os.mkdir("etl_steps/relational_model")

    return True


def get_list_of_industries():
    df1 = pd.read_csv("etl_steps/data_temp/base_nsa_fact.csv")
    df2 = pd.read_csv("etl_steps/data_temp/base_sa_fact.csv")

    nsa_array = df1["industry_id"].astype(str).str.zfill(8).unique()
    sa_array = df2["industry_id"].astype(str).str.zfill(8).unique()
    total_array = np.unique(np.concatenate([nsa_array, sa_array]))

    dfn = pd.read_csv("https://download.bls.gov/pub/time.series/sm/sm.industry", sep="\t")
    dfn["industry_code"] = dfn["industry_code"].astype(str).str.zfill(8)
    name_map = {code:name for (code,name) in zip(dfn["industry_code"], dfn["industry_name"])}

    df = pd.DataFrame({"code": total_array})
    df["name"] = df["code"].map(name_map)
    
    return df


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


def create_hierarchy(df):
    print("Creating new hierarchy...")
    df["level"] = pd.Series([code_reader(c) for c in df["code"]])

    row_list = [[""]*12 for l in range(len(df))]

    for i,r in df.iterrows():
        row_list[i][r["level"]] = r["code"]
        row_list[i][r["level"]+1] = r["name"]

    df = pd.DataFrame(columns=["L1_code", "L1_name", "L2_code", "L2_name", "L3_code", "L3_name", "L4_code", "L4_name", "L5_code", "L5_name", "L6_code", "L6_name"], data=row_list)

    print("Created new hierarchy!")
    return df


def place_code(df, code, hd, other_dict, level_dict):
    col_list = ["L{}_code".format(k) for k in range(1,7)]
    position = int(level_dict[code][1])-1
    
    current_col = col_list[position]
    children_col = col_list[position+1]
    level = "L"+str(position+1)
    
    other = other_dict[code]
    children_list = [c for c in df.loc[df[current_col]==code, children_col].unique() if c!=other]
    
    hd[level].append({code: {"children": children_list, "other": other}})
    
    return hd


def wrangle_hierarchy(df_input, df_list):
    df = df_input.copy()
    df = df.fillna("")
    df = df[~df.index.isin([0,1,2,3,4,15])]

    # Fill L6 from L5
    for i, r in df.iterrows():
        if r["L5_code"]=="" and r["L6_code"]!="":
            df.loc[i, "L5_code"] = r["L6_code"][:6] + "00"
            df.loc[i, "L5_name"] = r["L6_name"]
            
        elif r["L5_code"]!="" and r["L6_code"]=="":
            df.loc[i, "L6_code"] = r["L5_code"][:6] + "99"
            df.loc[i, "L6_name"] = r["L5_name"]


    # Fill L5 and L6 from L4
    for i,r in df.iterrows():
        if r["L4_code"]!="" and r["L5_code"]=="" and r["L6_code"]=="":
            df.loc[i, "L5_code"] = r["L4_code"][:5] + "900"
            df.loc[i, "L5_name"] = r["L4_name"]
            df.loc[i, "L6_code"] = r["L4_code"][:5] + "999"
            df.loc[i, "L6_name"] = r["L4_name"]
            
    # Forward fill for L4
    current_code = ""
    current_name = ""
    for i,r in df.iterrows():
        if r["L4_code"]!="" and r["L5_code"]!="" and r["L6_code"]!="":
            current_code = r["L4_code"]
            current_name = r["L4_name"]
        
        elif r["L4_code"]=="" and r["L5_code"]!="" and r["L6_code"]!="":
            df.loc[i, "L4_code"] = current_code
            df.loc[i, "L4_name"] = current_name


    # Manual Changes
    df.loc[5, ["L2_code", "L2_name", "L3_code", "L3_name", "L4_code", "L4_name", "L5_code", "L5_name", "L6_code", "L6_name"]] = ["10900000", "Mining and Logging", "10990000", "Mining and Logging", "10999000", "Mining and Logging", "10999900", "Mining and Logging", "10999999", "Mining and Logging"]
    df.loc[6, ["L3_code", "L3_name", "L4_code", "L4_name"]] = ["10110000", "Logging", "10119000", "Logging"]
    df.loc[16, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["20900000", "Construction", "20990000", "Construction"]
    df.loc[30, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["30900000", "Manufacturing", "30990000", "Manufacturing"]
    df.loc[31, ["L3_code", "L3_name"]] = ["31990000", "Durable Goods"]
    df.loc[75, ["L3_code", "L3_name"]] = ["32990000", "Non-Durable Goods"]
    df.loc[108, ["L2_code", "L2_name", "L3_code", "L3_name", "L4_code", "L4_name", "L5_code", "L5_name", "L6_code", "L6_name"]] = ["40900000", "Trade, Transportation, and Utilities", "40990000", "Trade, Transportation, and Utilities", "40999000", "Trade, Transportation, and Utilities", "40999900", "Trade, Transportation, and Utilities", "40999999", "Trade, Transportation, and Utilities"]
    df.loc[109, ["L3_code", "L3_name"]] = ["41990000", "Wholesale Trade"]
    df.loc[126, ["L3_code", "L3_name"]] = ["42990000", "Retail Trade"]
    df.loc[158, ["L3_code", "L3_name"]] = ["43990000", "Transportation, Warehousing, and Utilities"]
    df.loc[180, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["50900000", "Information", "50990000", "Information"]
    df.loc[195, ["L3_code", "L3_name"]] = ["55990000", "Financial Activities"]
    df.loc[223, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["60900000", "Professional and Business Services", "60990000", "Professional and Business Services"]
    df.loc[249, ["L3_code", "L3_name"]] = ["65990000", "Education and Health Services"]
    df.loc[279, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["70900000", "Leisure and Hospitality", "70990000", "Leisure and Hospitality"]
    df.loc[302, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["80900000", "Other Services", "80990000", "Other Services"]
    df.loc[317, ["L2_code", "L2_name", "L3_code", "L3_name"]] = ["90900000", "Government", "90990000", "Government"]


    # Fill L4, L5 and L6 from L3
    for i,r in df.iterrows():
        if r["L3_code"]!="" and r["L4_code"]=="" and r["L5_code"]=="" and r["L6_code"]=="":
            df.loc[i, "L4_code"] = r["L3_code"][:4] + "9000"
            df.loc[i, "L4_name"] = r["L3_name"]
            df.loc[i, "L5_code"] = r["L3_code"][:4] + "9900"
            df.loc[i, "L5_name"] = r["L3_name"]
            df.loc[i, "L6_code"] = r["L3_code"][:4] + "9999"
            df.loc[i, "L6_name"] = r["L3_name"]


    # Forward fill for L3, L2 and L1
    for i, r in df.iterrows():
        for c in r.keys():
            if r[c]=="":
                df.loc[i, c] = np.nan
                
    for c in ["L3_code", "L3_name", "L2_code", "L2_name", "L1_code", "L1_name"]:
        df[c] = df[c].ffill()


    # Integrate original code and level
    df2 = df_list.copy()
    df2 = df2[~df2.index.isin([0,1,2,3,4,15])]

    df["original_code"] = df2["code"].astype(str)
    df["level"] = ""

    for i,r in df.iterrows():
        for c in ["L1_code", "L2_code", "L3_code", "L4_code", "L5_code", "L6_code"]:
            if r[c] ==  r["original_code"]:
                df.loc[i, "level"] = c

    df3 = df.copy().rename(columns={"original_code": "industry_id"})
    df3 = df3[["industry_id", "L1_code", "L1_name","L2_code", "L2_name", "L3_code", "L3_name", "L4_code", "L4_name", "L5_code", "L5_name", "L6_code", "L6_name"]]
    df3.to_csv("etl_steps/relational_model/bls_dim_industry.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
    print("Saved new hierarchy!")

    other_dictionary = {r["original_code"]:r["L6_code"] for i,r in df.iterrows() if r["original_code"]!=r["L6_code"]}
    level_dictionary = {r["original_code"]:r["level"] for i,r in df.iterrows() if r["level"]!="L6_code"}

    hierarchy_dictionary = {"L{}".format(k):[] for k in range(1,6)}

    for c in level_dictionary.keys():
        hierarchy_dictionary = place_code(df.copy(), c, hierarchy_dictionary, other_dictionary, level_dictionary)

    with open("etl_steps/data_temp/hierarchy_dictionary.json","w") as file:
        file.write(json.dumps(hierarchy_dictionary, indent=2, sort_keys=True))

    print("Wrote new hierarchy dictionary!")

    return True


if __name__ == "__main__":
    init()
    df_list = get_list_of_industries()
    df = create_hierarchy(df_list)
    wrangle_hierarchy(df, df_list)
    print("All completed!")