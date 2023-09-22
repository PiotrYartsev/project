import sqlite3 as sl
from tqdm import tqdm
import os
from Rucio_functions import RucioFunctions

class RucioDataset():

    @classmethod
    def create_table(self):
        self.con.execute("CREATE TABLE {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, scope TEXT NOT NULL, rse TEXT NOT NULL, adler32 TEXT NOT NULL, timestamp INTEGER NOT NULL, filenumber INTEGER NOT NULL, location TEXT NOT NULL, has_replicas INTEGER NOT NULL);".format(self.dataset_table_name))
    
    @classmethod
    def check_if_exist(cls, dataset_and_scope,local_database_dataset_data):
        dataset=dataset_and_scope[1]
        scope=dataset_and_scope[0]
        output=[]
        number_of_files_in_rucio=RucioFunctions.count_files_func(scope,dataset)
        
        for item in local_database_dataset_data:
            Found=False
            if item[1]==dataset and item[0]==scope:
                number_of_files_in_local=item[2]
                Found=True
                break
        if Found==False:
            output=[dataset_and_scope,False]
        else:
            if number_of_files_in_local==number_of_files_in_rucio:
                output=[dataset_and_scope,True]

            else:

                output=[dataset_and_scope,False]

        return output
    
    @classmethod
    def fill_data_from_local(cls, dataset_in_local):
        LocalRucioDataset=sl.connect('local_rucio_database.db')
        dataset=dataset_in_local[1]
        scope=dataset_in_local[0]
        changed_dataset_name=LocalRucioDataset.execute("SELECT table_name FROM dataset WHERE scope=? AND name=?",(scope,dataset)).fetchall()
        #changed_dataset_name=LocalRucioDataset.execute(f"SELECT table_name FROM dataset WHERE scope= '{scope}'").fetchall()
        if len(changed_dataset_name)>1:
            print("Error: more than one dataset with the same name and scope")
            exit(1)
        else:
            changed_dataset_name=changed_dataset_name[0][0]
            #open that table and read the data
            data_in_local_database=LocalRucioDataset.execute("SELECT * FROM {}".format(changed_dataset_name)).fetchall()
            #fill the FileMetadata class
            ouptut_list=[]
            for i in range(len(data_in_local_database)):
                output=FileMetadata(
                    name=data_in_local_database[0][1],
                    dataset=dataset,
                    scope=data_in_local_database[0][2],
                    rse=data_in_local_database[0][3],
                    adler32=data_in_local_database[0][4],
                    timestamp=data_in_local_database[0][5],
                    filenumber=data_in_local_database[0][6],
                    location=data_in_local_database[0][7],
                    has_replicas=data_in_local_database[0][8])
                ouptut_list.append(output)
            return ouptut_list

    @classmethod
    def extract_from_rucio(cls,dataset):
        dataset_name=dataset[1]
        scope=dataset[0]
        print(RucioFunctions.list_files_dataset(scope=scope,name=dataset_name))
    

class FileMetadata:
    def __init__(self, name,dataset,rse, scope, adler32,timestamp,filenumber,  location, has_replicas):
        self.name = name
        self.dataset=dataset
        self.rse = rse
        self.scope = scope
        self.adler32 = adler32
        self.timestamp=timestamp
        self.filenumber=filenumber
        self.location = location
        self.has_replicas = has_replicas
        
class CustomDataStructure:
    def __init__(self):
        # Initialize dictionaries for each metadata category
        self.name_index = {}
        self.rse_index = {}
        self.dataset_index={}
        self.scope_index = {}
        self.adler32_index = {}
        self.timestamp_index = {}
        self.filenumber_index = {}
        self.location_index = {}
        self.has_replicas_index = {}

    def add_item(self, item):
        # Create a FileMetadata instance and add it to all relevant indexes
        metadata = FileMetadata(
            item["name"],
            item["dataset"],
            item["rse"],
            item["scope"],
            item["adler32"],
            item["timestamp"],
            item["filenumber"],
            item["location"],
            item["has_replicas"]
        )
        self.name_index[item["name"]] = metadata
        self.dataset_index[item["dataset"]] = metadata
        self.rse_index[item["rse"]] = metadata
        self.scope_index[item["scope"]] = metadata
        self.adler32_index[item["adler32"]] = metadata
        self.timestamp_index[item["timestamp"]] = metadata
        self.filenumber_index[item["filenumber"]] = metadata
        self.location_index[item["location"]] = metadata
        self.has_replicas_index[item["has_replicas"]] = metadata

    def find_by_metadata(self, metadata_category, value):
        # Retrieve items by a specific metadata category and value
        if metadata_category == "name":
            return self.name_index.get(value)
        elif metadata_category == "dataset":
            return self.dataset_index.get(value)
        elif metadata_category == "rse":
            return self.rse_index.get(value)
        elif metadata_category == "scope":
            return self.scope_index.get(value)
        elif metadata_category == "adler32":
            return self.adler32_index.get(value)
        elif metadata_category == "timestamp":
            return self.timestamp_index.get(value)
        elif metadata_category == "filenumber":
            return self.filenumber_index.get(value)
        elif metadata_category == "location":
            return self.location_index.get(value)
        elif metadata_category == "has_replicas":
            return self.has_replicas_index.get(value)