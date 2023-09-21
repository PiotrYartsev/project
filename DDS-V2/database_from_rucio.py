import signal
import sqlite3 as sl
import re
from tqdm import tqdm
#from Rucio_functions import list_files_dataset, list_replicas, count_files_func,list_scopes,list_dataset,list_replicas_batch
import sys
import traceback
import os

class RucioDataset(object):
    def __init__(self, dataset_table_name):
        self.dataset_table_name = dataset_table_name
        self.con = sl.connect('Rucio_data_LUND_GRIDFTP.db')
        self.create_table()
    @classmethod
    def create_table(self):
        self.con.execute("CREATE TABLE {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, scope TEXT NOT NULL, rse TEXT NOT NULL, adler32 TEXT NOT NULL, timestamp INTEGER NOT NULL, filenumber INTEGER NOT NULL, location TEXT NOT NULL, has_replicas INTEGER NOT NULL);".format(self.dataset_table_name))
    """
    @classmethod
    def append_to_table(self, data):
        append_to_table_output=[]
        for key in data:
            name_all=key["name"]
            scope_all=key["scope"]
            adler32_all=key["adler32"]
            rse_all=key["rse"]
            location_all=key["location"]
            has_replicas_all=key["has_replicas"]
            for i in range(len(name_all)):
                name=name_all[i]
                scope=scope_all[i]
                adler32=adler32_all[i]
                rse=rse_all[i]
                location=location_all[i]
                has_replicas=has_replicas_all[i]
                namesplit=name.split("_")
                timestamp=namesplit[-1].replace(".root","").replace("t","")
                filenumber=namesplit[-2].replace("run","")
                append_to_table_output.append((name, scope, rse, adler32, timestamp, filenumber, location,has_replicas))
        self.con.executemany("INSERT INTO {} (name, scope, rse, adler32, timestamp, filenumber, location, has_replicas) VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(self.dataset_table_name), append_to_table_output)
        self.con.commit()
    @classmethod
    def read_rucio_dataset_files_and_extract_metadata(self, file_data_from_dataset_output, scope):
        file_list = [(file_data["scope"], file_data["name"]) for file_data in file_data_from_dataset_output]
        replicas = list_replicas_batch(file_list)
        data = []
        for replica in replicas:
            replicas_count = len(replica["rses"])
            for j in range(replicas_count):
                file_info = {}
                file_info["name"] = []
                file_info["scope"] = []
                file_info["adler32"] = []
                file_info["rse"] = []
                file_info["location"] = []
                file_info["has_replicas"] = []
                file_info["name"].append(replica["name"])
                file_info["scope"].append(replica["scope"])
                file_info["adler32"].append(replica["adler32"])
                file_info["rse"].append(list(replica["rses"].keys())[j])
                file_info["location"].append(replica["rses"][list(replica["rses"].keys())[j]][0])
                file_info["has_replicas"].append(replicas_count-1)
                data.append(file_info)
        return data
    


    @classmethod
    def process_dataset(self, scope, name):
        files_in_dataset = list_files_dataset(scope, name)
        metadata = self.read_rucio_dataset_files_and_extract_metadata(files_in_dataset, scope)
        self.append_to_table(metadata)
    """

class RucioFileMetadata(RucioDataset):
    name: str
    scope: str
    adler32: str
    rse: str
    location: str
    has_replicas: int