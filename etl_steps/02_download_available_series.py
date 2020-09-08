import pandas as pd
import json
import os
import csv
import requests


def init():
    if not os.path.isdir("etl_steps/data_temp/download_json_response"):
        os.mkdir("etl_steps/data_temp/download_json_response")

    if not os.path.isdir("etl_steps/data_temp/download_series_by_state"):
        os.mkdir("etl_steps/data_temp/download_series_by_state")
    
    with open("etl_steps/data_temp/valid_series_by_state.json","r") as file:
        valid_series = json.loads(file.read())

    return valid_series


def download_series(valid_series, start_year):
    for entry in valid_series:
        code, name, series = entry["code"], entry["name"], entry["series"]
        print("Preparing "+name+"...")

        list_chunks = [series[i:i+50] for i in range(0,len(series)//50*50,50)]
        if series[len(series)-len(series)%50:] != []:
            list_chunks.append(series[len(series)-len(series)%50:])

        with open("etl_steps/data_temp/download_json_response/"+code+"_"+name+"_00.txt","w") as file:
            for index, chunk in enumerate(list_chunks):
                file.write("Chunk {}:\n".format(index))
                file.write(str(chunk)+"\n")

        print("Number of Series: {}".format(len(series)))
        print("Number of Chunks: {}".format(len(list_chunks)))

        name = name.replace(" ","_")

        if code+"_"+name+".csv" not in os.listdir("etl_steps/data_temp/download_series_by_state"):

            for index, series in enumerate(list_chunks):
                n = len(list_chunks)
                i = index
                print("\nChunk {}/{} | {:.2f}%".format(i+1, n, (i+1)/n*100))

                headers = {'Content-type': 'application/json'}
                data = json.dumps(
                    {
                        "seriesid": series, 
                        "registrationkey": "47391590000a4d2583a0efe342d66cf8", 
                        "startyear": start_year, 
                        "endyear": "2020"
                    }
                )
                p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

                with open("etl_steps/data_temp/download_json_response/"+code+"_"+name+"_"+str(i+1)+"of"+str(n)+".json","w") as file:
                    file.write(p.text)

                json_data = json.loads(p.text)
                print(json_data["status"])
                print(json_data["message"][0:2])

                for series_obj in json_data["Results"]["series"]:
                    result = series_obj["data"]
                    data = {"seriesID": series_obj["seriesID"], "year": [], "period": [], "value": []}

                    for i in range(len(result)):
                        for k in ["year", "period", "value"]:
                            data[k].append(result[i][k])

                    df = pd.DataFrame(data)

                    df = df.rename(columns={"period":"month"})
                    df["month"] = df["month"].str.replace("M","")

                    for c in ["year", "month"]:
                        df[c] = df[c].astype(int)

                    df["value"] = df["value"].astype(float)

                    df.to_csv("etl_steps/data_temp/download_series_by_state/{}.csv".format(code+"_"+name), quoting=csv.QUOTE_NONNUMERIC, mode="a", index=False, header=False)
        else:
            df = pd.read_csv("etl_steps/data_temp/download_series_by_state/{}.csv".format(code+"_"+name))

    return True


if __name__ == "__main__":
    ##### CHANGE YEAR HERE ###
    start_year = "2019"
    ##########################
    
    VALID_SERIES_BY_STATE = init()
    download_series(VALID_SERIES_BY_STATE, start_year)
    