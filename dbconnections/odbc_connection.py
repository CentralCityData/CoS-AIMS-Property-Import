import pyodbc
import os
import pandas as pd

class ODBCConnection(object):
    """Creates an open database connection.
    
    Args:
        conn_string (str): The connection string to the database.
        
    Returns:
        Instance (ODBCConnection)"""
    def __init__(self, conn_string):
        self.query_directory = f"{os.getcwd()}/queries/"
        self.conn_string = conn_string
        self.conn = pyodbc.connect(self.conn_string)


    def _load_query(self, query_fname):
        """Loads a query"""

        with open(f"{self.query_directory}/{query_fname}.sql") as inf:
            return inf.read()


    def fetch_tables(self):

        q = self.load_query("query_tables")
        return self.conn.execute(q).fetchall()


    def perform_query(self, query_fname):
        q = self._load_query(query_fname)
        self.conn.execute(q)
        self.conn.commit()


    def load_query_as_dataframe(self, query_fname):

        q = self._load_query(query_fname)
        return pd.read_sql(q, self.conn)

