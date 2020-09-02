import pandas as pd
import numpy as np
import csv
import os
import shutil
from util import HIERARCHY_DICT, STATE_NAMES


def list_industries(m, s, df):
    sub_df = df[(df["time_id"]==m) & (df["state_id"]==s)]
    
    return list(sorted(sub_df["industry_id"].unique()))


def get_children_sum(m, s, ind, df, hd, level):
    sub_list = hd[level][ind]["children"] + [hd[level][ind]["other"]]
    conds = (df["time_id"]==m) & (df["state_id"]==s) & (df["industry_id"].isin(sub_list))
    
    return df[conds]["employees"].sum()

def start(keyword):
    dtype = {"industry_id": "object", 
             "time_id": "object", 
             "state_id": "int",
             "employees": "float" 
            }

    df = pd.read_csv("../data_output/bls_{}_fact.csv".format(keyword), dtype=dtype)
    key_path = "partial_" + keyword

    if os.path.isdir(key_path):
        shutil.rmtree(key_path)
        os.mkdir(key_path)
    else:
        os.mkdir(key_path)

    all_months = sorted(list(df["time_id"].unique()))
    all_states = sorted(list(df["state_id"].unique()))
    all_industries = sorted(list(df["industry_id"].unique()))
    
    return (df, all_months, all_states, all_industries)


def process_level(level, df, hierarchy_dict, state_names, sa_adj):
    df_new = pd.DataFrame([], columns=list(df.keys()))

    new_hierarchy = {}
    for l in ["L1", "L2", "L3", "L4", "L5"]:
        new_hierarchy[l] = {list(e.keys())[0]:{"children": e[list(e.keys())[0]]["children"], "other": e[list(e.keys())[0]]["other"]} for e in hierarchy_dict[l]}
    
    entries = {l:[list(entry.keys())[0] for entry in hierarchy_dict[l]] for l in ["L1", "L2", "L3", "L4", "L5"]}
    df_level = {l:df[df["industry_id"].isin(entries[l])] for l in ["L1", "L2", "L3", "L4", "L5"]}
    #length = {l:len(df_level[l]) for l in ["L1", "L2", "L3", "L4", "L5"]}
    
    a = 0
    for m in all_months: #for m in all_months:
        for s in all_states: #for s in all_states:
            a += 1
            print("({:.4f})% | Processing {}-{}, {} ({})".format(a/(len(all_months)*len(all_states))*100, m[:4], m[-2:], state_names[str(s).zfill(2)], s))
            for ind in list_industries(m, s, df_level[level]):
                #a += 1
                conds = (df_level[level]["time_id"]==m) & (df_level[level]["state_id"]==s) & (df_level[level]["industry_id"]==ind)
                assert len(df_level[level][conds]) <= 1

                value = int(df_level[level].loc[conds, "employees"].sum())
                #print("\nValue = {:,}".format(value))

                children_sum = int(get_children_sum(m, s, ind, df, new_hierarchy, level))
                #print("Children Sum = {:,}".format(children_sum))

                difference = value - children_sum
                #print("Difference = {:,}".format(difference))

                if value - 1 > children_sum:
                    other = new_hierarchy[level][ind]["other"]
                    df_row = pd.DataFrame([[m, s, other, difference]], columns=list(df.keys()))
                    filename = "partial_{}/{}_{}.csv".format(sa_adj.lower(), sa_adj,level)
                    header = list(df_row.keys()) if not os.path.isfile(filename) else False
                    df_row.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC, header=header, mode="a")
                    df_new = df_new.append(df_row, ignore_index=True)
                    
                    #print("({:.4f}%) {}-{}, {} ({}), {} ... ADDED {:,} to {}".format(a/length[level]*100, m[:4], m[-2:], state_names[str(s).zfill(2)], s, ind, difference, other))
                #else:
                    #print("({:.4f}%) {}-{}, {} ({}), {}".format(a/length[level]*100, m[:4], m[-2:], state_names[str(s).zfill(2)], s, ind))
    
    print("\n{} PROCESSED SUCCESSFULLY!\n".format(level))
    
    return df_new


if __name__ == "__main__":
    for kw in ["nsa", "sa"]:
        df, all_months, all_states, all_industries = start(kw)

        for level in ["L5", "L4", "L3", "L2", "L1"]:
            df = pd.concat([df, process_level(level, df, HIERARCHY_DICT, STATE_NAMES, kw.upper())], ignore_index=True)
            
        df.to_csv("new_bls_{}_fact.csv".format(kw), index=False, quoting=csv.QUOTE_NONNUMERIC)
        print("Completed {}!".format(kw.upper()))