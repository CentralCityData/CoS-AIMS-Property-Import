import requests
import urllib
import json
import sys # Only for filtering flag. Not Needed.
import math

from tqdm import tqdm # Only for progress bar. Not Needed

import pandas as pd # Only for exporting. Not needed.


class ArcGisAPIPuller():
    def __init__(self, filter_for_syr=True):
        self.URL_BASE = "https://ongisweb.ongov.net/arcgis/rest/services/Base_Maps/Onondaga_County_Base_Map/MapServer/19/query?"
        self.PER_PAGE = 10000
        self.params = {'where': '1=1',
                       'f': 'geojson',
                       'outFields': '*',
                       'orderByFields': 'OBJECTID',
                       'resultRecordCount': f'{self.PER_PAGE}'}
        if filter_for_syr:
            self.params["where"] = "MCDNAME='SYRACUSE CITY'"
        self.total_records = self.get_count()


    def pull_data(self):

        """Pulls all of the data by first counting the pages and interating through the count.
        
        Benefit, can see progress in cmd line.
        """

        catch = [] # Catch all pages in single data structure. (Not needed, but convient for crushing to dataframe.)
        for pg in tqdm(range(math.ceil(self.total_records / self.PER_PAGE))):
            self.params["resultOffset"] = pg * self.PER_PAGE
            res = self.get_page()
            catch.extend(res["features"])

        df = pd.json_normalize(catch)

        # If number of records in return matches
        # the
        try:
            assert self.total_records == df.shape[0]
        except  AssertionError:
            print(f"[WARNING!] Number of records ({df.shape[0]}) in resulting DataFrame DOES NOT MATCH number of records from ArcGIS Server ({self.total_records})")
        return pd.json_normalize(catch)


    def get_count(self):
        """Returns count of total records.
        
        NOTE: Record count will be changed based on 'where' condition in params.
        """

        self.params["returnCountOnly"] = "true"
        r = requests.get(f"{self.URL_BASE}{urllib.parse.urlencode(self.params)}")
        self.params["returnCountOnly"] = "false"

        return json.loads(r.text)["count"]


    def get_page(self, fields="[*]"):
        """Returns a single page as json from URL endpoint."""
        
        query_url = f"{self.URL_BASE}{urllib.parse.urlencode(self.params)}"
        r = requests.get(query_url)
        return json.loads(r.text)

    
if __name__ == "__main__":
    pass