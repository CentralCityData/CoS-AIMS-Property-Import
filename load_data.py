from ArcGisAPIPuller import ArcGisAPIPuller
from dbconnections.odbc_connection import ODBCConnection

import pandas as pd

def load_data():

    CACHE_DIR = "./cached_files"
    COUNTY_CACHE_FNAME = "county_pull.csv"
    RPS_CACHE_FNAME = "rps_pull.csv"
    RENTAL_REGISTRY_FNAME = "rr_pull.csv"
    CONNECTION_STRING_RPS = "Driver={SQL Anywhere 12};Host=10.250.78.190;Server=sql_syracuse;port=2638;db=rps;uid=DBA;pwd=sybase;Trusted_Connection=yes;"
    CONNECTION_STRING_IPS = "Driver={SQL Server};Host=10.250.78.161;db=Building;user=ips_report;pwd=cusereport"
    OUTPUT_DIR = "./outputs"

    try:
        df_county = pd.read_csv(f"{CACHE_DIR}/{COUNTY_CACHE_FNAME}")
    except FileNotFoundError:
        puller = ArcGisAPIPuller()
        df_county = puller.pull_data()
        assert puller.total_records == df_county.shape[0]
        df_county.to_csv(f"{CACHE_DIR}/{COUNTY_CACHE_FNAME}", index=False)

    try:
        df_rps = pd.read_csv(f"{CACHE_DIR}/{RPS_CACHE_FNAME}")
    except FileNotFoundError:
        puller = ODBCConnection(CONNECTION_STRING_RPS)
        df_rps = puller.load_query_as_dataframe("hamer_pull")
        df_rps.to_csv(f"{CACHE_DIR}/{RPS_CACHE_FNAME}", index=False)

    # try:
    #     rr_data = pd.read_csv(f"{CACHE_DIR}/{RENTAL_REGISTRY_FNAME}")
    # except FileNotFoundError:
    #     puller = ODBCConnection(CONNECTION_STRING_IPS)
    #     rr_data = puller.load_query_as_dataframe("rr_pull")
    #     rps_data.to_csv(f"{CACHE_DIR}/{RENTAL_REGISTRY_FNAME}", index=False)


    return (df_rps, df_county)

if __name__ == "__main__":
    load_data()