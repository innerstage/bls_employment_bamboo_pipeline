import pandas as pd
import json
import csv
import os
import requests

from bamboo_lib.helpers import grab_connector, query_to_df, grab_connector
from bamboo_lib.logger import logger, logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter, LoopHelper

from util import VALID_SERIES_BY_STATE, STATE_NAMES, SUPERSECTOR_NAMES, INDUSTRY_NAMES


class InitializationStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Initialization Step...")
        
        if not os.path.isdir("data_temp"):
            os.mkdir("data_temp")
        
        if not os.path.isdir("data_output"):
            os.mkdir("data_output")

        iterable_series = iter(VALID_SERIES_BY_STATE)
        
        return iterable_series


class PreparationStep(PipelineStep):
    def run_step(self, prev, params):
        code, name, series = prev["code"], prev["name"], prev["series"]
        logger.info("Preparing "+name+"...")

        list_chunks = [series[i:i+50] for i in range(len(series)//50)]
        if series[len(series)-len(series)%50:] != []:
            list_chunks.append(series[len(series)-len(series)%50:])

        logger.info("Number of Series: {}".format(len(series)))
        logger.info("Number of Chunks: {}".format(len(list_chunks)))

        name = name.replace(" ","_")
        
        return (list_chunks, code, name)


class DownloadAndTransformStep(PipelineStep):
    def run_step(self, prev, params):
        list_chunks, code, name = prev

        if code+"_"+name+".csv" not in os.listdir("data_temp"):

            for index, series in enumerate(list_chunks):
                n = len(list_chunks)
                i = index
                logger.info("\nChunk {}/{} | {:.2f}%".format(i+1, n, (i+1)/n*100))

                headers = {'Content-type': 'application/json'}
                data = json.dumps({"seriesid": series, "registrationkey": "edbfb0418333474da9abddd85d1982ff", "startyear": "2001", "endyear": "2020"})
                p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

                json_data = json.loads(p.text)
                logger.info(json_data["status"])
                logger.info(json_data["message"][0:2])

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

                    df.to_csv("data_temp/{}.csv".format(code+"_"+name), quoting=csv.QUOTE_NONNUMERIC, mode="a", index=False, header=False)
        else:
            df = pd.read_csv("data_temp/{}.csv".format(code+"_"+name))

        return True


class ConcatenationAndMappingStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Concatenation Step...")

        filenames = [f for f in os.listdir("data_temp") if ".csv" in f]
        df_list = []

        for filename in filenames:
            df = pd.read_csv("data_temp/"+filename, names=["series_id", "year", "month", "value"])
            df_list.append(df)

        df = pd.concat(df_list)

        df["seasonal_adjustment"] = df["series_id"].str[2:3].map({"S": 1, "U": 0})
        df["state_code"] = df["series_id"].str[3:5]
        df["state_name"] = df["series_id"].str[3:5].map(STATE_NAMES)
        df["supersector_code"] = df["series_id"].str[10:12]
        df["supersector_name"] = df["series_id"].str[10:12].map(SUPERSECTOR_NAMES)
        df["industry_code"] = df["series_id"].str[10:18]
        df["industry_name"] = df["series_id"].str[10:18].map(INDUSTRY_NAMES)

        df = df.dropna()
        df = df[["series_id", "year", "month", "seasonal_adjustment", "state_code", "state_name", "supersector_code", "supersector_name", "industry_code", "industry_name", "value"]]
        df = df.rename(columns={"value": "employees (in thousands)"})
        df.to_csv("data_output/single_output.csv", quoting=csv.QUOTE_NONNUMERIC, index=False)
        logger.info("Completed!")

        return df


class SingleFilePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("concat_only", dtype=bool, default_value=False)
        ]
    
    @staticmethod
    def steps(params):
        init_step = InitializationStep()
        prep_step = PreparationStep()
        dl_and_transform = DownloadAndTransformStep()
        concat_and_map = ConcatenationAndMappingStep()
        
        sub_steps = [prep_step, dl_and_transform]
        steps = [init_step, LoopHelper(iter_step=init_step, sub_steps=sub_steps), concat_and_map]
        
        if params.get("concat_only"):
            steps = [concat_and_map]
            
        return steps


if __name__ == "__main__":
    single_file_pipeline = SingleFilePipeline()
    single_file_pipeline.run(
        {
            "concat_only": False
        }
    )