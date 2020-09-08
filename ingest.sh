. .env
cp etl_steps/relational_model/schema.xml datausa-tesseract/
cd etl_steps/relational_model
clickhouse-client --password=$DB_PW --query="DROP DATABASE bls"
echo "Dropped database..."
clickhouse-client --password=$DB_PW --query="CREATE DATABASE bls"
clickhouse-client --password=$DB_PW --query="CREATE TABLE bls.dim_time_bls (time_id Int32, date_name String, year Int32, quarter String, month Int32, month_name String) ENGINE = Log"
clickhouse-client --password=$DB_PW --query="CREATE TABLE bls.dim_state_bls (state_id Int32,state_name String, fips_code String) ENGINE = Log"
clickhouse-client --password=$DB_PW --query="CREATE TABLE bls.dim_industry_bls (industry_id Int32, L1_code String, L1_name String, L2_code String, L2_name String, L3_code String, L3_name String, L4_code String, L4_name String, L5_code String, L5_name String, L6_code String, L6_name String) ENGINE = Log"
clickhouse-client --password=$DB_PW --query="CREATE TABLE bls.fact_sa_bls (time_id Int32, state_id Int32,industry_id Int32, employees Float64) ENGINE = Log"
clickhouse-client --password=$DB_PW --query="CREATE TABLE bls.fact_nsa_bls (time_id Int32, state_id Int32,industry_id Int32, employees Float64) ENGINE = Log"
echo "Created tables..."
tail -n +2 bls_dim_time.csv | clickhouse-client --password=$DB_PW --query="INSERT INTO bls.dim_time_bls FORMAT CSV"
tail -n +2 bls_dim_state.csv | clickhouse-client --password=$DB_PW --query="INSERT INTO bls.dim_state_bls FORMAT CSV"
tail -n +2 bls_dim_industry.csv | clickhouse-client --password=$DB_PW --query="INSERT INTO bls.dim_industry_bls FORMAT CSV"
tail -n +2 bls_fact_sa.csv | clickhouse-client --password=$DB_PW --query="INSERT INTO bls.fact_sa_bls FORMAT CSV"
tail -n +2 bls_fact_nsa.csv | clickhouse-client --password=$DB_PW --query="INSERT INTO bls.fact_nsa_bls FORMAT CSV"
echo "All files ingested... Completed!"
sudo systemctl restart tesseract-olap
sudo systemctl restart nginx