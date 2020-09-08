rm -rf etl_steps/data_temp
. ./py3/bin/activate
python etl_steps/01_scrape_available_series_list.py
python etl_steps/02_download_available_series.py
python etl_steps/03_concatenate_and_map_dfs.py
python etl_steps/04_create_industry_hierarchy.py
python etl_steps/05_Run_filling_algorithm.py