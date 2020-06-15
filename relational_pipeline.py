import pandas as pd
import numpy as np
import json
import csv
import os
import requests
from calendar import month_name

from bamboo_lib.helpers import grab_connector, query_to_df, grab_connector
from bamboo_lib.logger import logger, logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter, LoopHelper
from bamboo_lib.steps import LoadStep

from util import VALID_SERIES_BY_STATE, STATE_NAMES, SUPERSECTOR_NAMES, INDUSTRY_NAMES


class TimeDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Time Dimension Step...")

        df = pd.DataFrame({"date": pd.date_range("2007-01", "2020-05", freq="M")})
        df["year"] = df.date.dt.year
        df["quarter"] = "Q" + df.date.dt.quarter.astype(str)
        df["month"] = df.date.dt.month
        df["month_name"] = df["month"].map({k:month_name[k] for k in range(1,13)})
        df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
        df["date_name"] = df["year"].astype(str) + " " + df["month_name"]

        df = df[["time_id", "date_name", "year", "quarter", "month", "month_name"]]

        df.to_csv("data_output/dim_time_bls.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

        return df


class StateDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("State Dimension Step...")
        
        state_codes = pd.Series(list(STATE_NAMES.keys()))

        state_dict = {"state_id": state_codes.astype(int), "state_name": state_codes.map(STATE_NAMES)}
        
        df = pd.DataFrame(state_dict)
        df["fips_code"] = "04000US" + df["state_id"].astype(str).str.zfill(2)

        df.to_csv("data_output/dim_state_bls.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class IndustryDimensionStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Industry Dimension Step...")
        
        industry_codes = pd.Series(list(INDUSTRY_NAMES.keys()))

        industry_dict = {
            "industry_id": industry_codes.astype(int),
            "supersector_code": industry_codes.str[0:2], 
            "supersector_name": industry_codes.str[0:2].map(SUPERSECTOR_NAMES),
            "industry_code": industry_codes,
            "industry_name": industry_codes.map(INDUSTRY_NAMES)
            }
        
        df = pd.DataFrame(industry_dict)

        df.to_csv("data_output/dim_industry_bls.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        return df


class SAFactTableStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("SA Fact Table Processing...")

        df = pd.read_csv("data_output/single_output.csv")

        df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
        df["state_id"] = df["state_code"].astype(int)
        df["industry_id"] = df["industry_code"].astype(int)

        df = df.rename(columns={"employees (in thousands)": "employees", "seasonal_adjustment": "seasonal_adjustment_id"})

        df = df[["time_id", "seasonal_adjustment_id", "state_id", "industry_id", "employees"]]
        df["employees"] = df["employees"] * 1000

        df_sa = df[df["seasonal_adjustment_id"]==1]
        df_sa = df_sa[["time_id", "state_id", "industry_id", "employees"]]
        df_sa["time_id"] = df_sa["time_id"].astype(int)
        df_sa.to_csv("data_output/bls_sa_fact.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

        print(df.head())

        return df_sa


class NSAFactTableStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("NSA Fact Table Processing...")

        df = pd.read_csv("data_output/single_output.csv")

        df["time_id"] = (df["year"].astype(str) + df["month"].astype(str).str.zfill(2)).astype(int)
        df["state_id"] = df["state_code"].astype(int)
        df["industry_id"] = df["industry_code"].astype(int)

        df = df.rename(columns={"employees (in thousands)": "employees", "seasonal_adjustment": "seasonal_adjustment_id"})

        df = df[["time_id", "seasonal_adjustment_id", "state_id", "industry_id", "employees"]]
        df["employees"] = df["employees"] * 1000

        df_nsa = df[df["seasonal_adjustment_id"]==0]
        df_nsa = df_nsa[["time_id", "state_id", "industry_id", "employees"]]
        df_nsa.to_csv("data_output/bls_nsa_fact.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

        return df_nsa


class RelationalPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("db", dtype=str),
            Parameter("ingest", dtype=bool, default_value=False)
        ]
    
    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("db"))

        time_step = TimeDimensionStep()
        state_step = StateDimensionStep()
        industry_step = IndustryDimensionStep()
        sa_fact_table_step = SAFactTableStep()
        nsa_fact_table_step = NSAFactTableStep()

        load_time = LoadStep("dim_time_bls", connector=db_connector, if_exists="drop", pk=["time_id"])
        load_state = LoadStep("dim_state_bls", connector=db_connector, if_exists="drop", pk=["state_id"])
        load_industry = LoadStep("dim_industry_bls", connector=db_connector, if_exists="drop", pk=["industry_id"])

        dtypes = {
            "time_id": "Int32", 
            "seasonal_adjustment_id": "UInt8", 
            "state_id": "Int32",
            "industry_id": "Int32",
            "employees": "Float64"
        }
        dtypes = {}

        load_fact_table_sa = LoadStep("fact_sa_bls", connector=db_connector, if_exists="drop", pk=["time_id"], dtype=dtypes)
        load_fact_table_nsa = LoadStep("fact_nsa_bls", connector=db_connector, if_exists="drop", pk=["time_id"], dtype=dtypes)

        steps = [time_step, load_time, state_step, load_state, industry_step, load_industry, sa_fact_table_step, load_fact_table_sa, nsa_fact_table_step, load_fact_table_nsa]

        if not params.get("ingest"):
            steps = [time_step, state_step, industry_step, sa_fact_table_step, nsa_fact_table_step]
            
        return steps


if __name__ == "__main__":
    relational_pipeline = RelationalPipeline()
    relational_pipeline.run(
        {
            "db": "clickhouse-local",
            "ingest": True
        }
    )