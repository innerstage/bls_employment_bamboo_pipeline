from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv
import pandas as pd
import os
import json
from util import STATE_NAMES


def init():
    if not os.path.isdir("etl_steps/data_temp"):
        os.mkdir("etl_steps/data_temp")

    if not os.path.isdir("etl_steps/data_temp/series_by_state"):
        os.mkdir("etl_steps/data_temp/series_by_state")

    return True


def table_to_csv(html, filename):

    soup = BeautifulSoup(html.read(), "lxml")
    table = soup.find("table")

    #print(table)

    output_rows = []
    for table_row in table.findAll('tr'):
        columns = table_row.findAll('th') + table_row.findAll('td')
        output_row = []
        for column in columns:
            output_row.append(column.text)
        output_rows.append(output_row)
    
    output_rows = [l for l in output_rows if l!=[]]
        
    df = pd.DataFrame(output_rows[1:], columns=output_rows[0])
    df["Series ID"] = df["Series ID"].str.replace("\n", "")

    df.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)

    return df


def scrape_all_available_series():

    print("Scraping all available series by state...")

    url = "https://www.bls.gov/sae/additional-resources/list-of-published-state-and-metropolitan-area-series/home.htm"
    html = urlopen(url)
    bsObj = BeautifulSoup(html.read(), "lxml")

    links = []
    for link in bsObj.find_all("a"):
        if "additional-resources" in link.get("href") and "home" not in link.get("href") and len(link.get("href"))>30:
            links.append("https://www.bls.gov" + link.get("href"))

    a = 0
    for state in links:
        a += 1
        html_st = urlopen(state)
        state_name = state[state.rfind("/")+1:].replace("htm","csv")

        print("{:.2f}% | Scraping available series from {}...".format(a/len(links)*100, state_name))
        table_to_csv(html_st, "etl_steps/data_temp/series_by_state/"+state_name)

    return list(os.listdir("etl_steps/data_temp/series_by_state"))


def filter_series(all_files):

    print("Filtering Statewide series...")
    all_dfs = []

    for filename in all_files:
        df = pd.read_csv("etl_steps/data_temp/series_by_state/" + filename)
        df["State"] = df["Series ID"].str[3:5].map(STATE_NAMES)
        all_dfs.append(df)


    df = pd.concat(all_dfs)
    df_nsa = df[(df["Area"]=="Statewide") & (df["Datatype"]=="All Employees, In Thousands") & (df["Adjustment Method"]=="Not Seasonally Adjusted")]
    df_nsa = df_nsa[["Series ID", "Industry", "State"]]
    df_nsa.to_csv("etl_steps/data_temp/NSA_series.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
    print("Completed NSA Series file!")

    df_sa = df[(df["Area"]=="Statewide") & (df["Datatype"]=="All Employees, In Thousands") & (df["Adjustment Method"]=="Seasonally Adjusted")]
    df_sa = df_sa[["Series ID", "Industry", "State"]]
    df_sa.to_csv("etl_steps/data_temp/SA_series.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
    print("Completed SA Series file!")

    return ("etl_steps/data_temp/NSA_series.csv", "etl_steps/data_temp/SA_series.csv")


def dictionary_generator(nsa_path, sa_path):
    print("Creating dictionary of valid series by state...")
    df1 = pd.read_csv(sa_path)
    df2 = pd.read_csv(nsa_path)

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

    df_json = json.dumps(valid_series, indent=2, sort_keys=True)
    with open("etl_steps/data_temp/valid_series_by_state.json", "w") as file:
        file.write(df_json)
        
    print("All completed!")

    return True


if __name__ == "__main__":
    init()
    all_available_series = scrape_all_available_series()
    nsa_path, sa_path = filter_series(all_available_series)
    dictionary_generator(nsa_path, sa_path)
