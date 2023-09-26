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
            # Create a new instance of CustomDataStructure
            list_of_metadata=[]

            # Loop over the data in the local database and add each item to the custom data structure
            for item in data_in_local_database:
                #print(item)
                metadata = FileMetadata(
                    name=item[1],
                    dataset=dataset,
                    scope=item[2],
                    rse=item[3],
                   
                    adler32=item[4],
                    timestamp=item[5],
                    filenumber=item[6],
                    location=item[7],
                    has_replicas=item[8]
                )
                list_of_metadata.append(metadata)

            # Return the custom data structure
            return list_of_metadata

    @classmethod
    def extract_from_rucio(cls,dataset,thread_count):
        dataset_name=dataset[1]
        scope=dataset[0]
        files=((RucioFunctions.list_files_dataset(scope=scope,name=dataset_name)))
        files=[(scope,file["name"]) for file in files]
        item=RucioFunctions.list_replicas_batch(files)
        #fill_me=FileMetadata
        
    @classmethod
    def combine_datastructure(cls,list_of_data_structures):
        # Create a new instance of CustomDataStructure
        combined_data_structure = CustomDataStructure()

        # Loop over the list of data structures and add their items to the combined data structure
        for data_structure in list_of_data_structures:
            combined_data_structure.add_items(data_structure.items())

        # Return the combined data structure
        return combined_data_structure


class FileMetadata:
    def __init__(self, name, dataset, scope, rse, adler32, timestamp, filenumber, location, has_replicas):
        self.name = name
        self.dataset = dataset
        self.scope = scope
        self.rse = rse
        self.adler32 = adler32
        self.timestamp = timestamp
        self.filenumber = filenumber
        self.location = location
        self.has_replicas = has_replicas

    def __getitem__(self, key):
        return getattr(self, key)
        
class CustomDataStructure:
    def __init__(self):
        # Initialize dictionaries (indexes) for each metadata category
        self.name_index = {}
        self.dataset_index = {}
        self.rse_index = {}
        self.scope_index = {}
        self.adler32_index = {}
        self.timestamp_index = {}
        self.filenumber_index = {}
        self.location_index = {}
        self.has_replicas_index = {}

    def add_item(self, item):
        # Create a FileMetadata instance
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
        
        # Append the metadata to the relevant indexes, using lists
        self._append_to_index(self.name_index, item["name"], metadata)
        self._append_to_index(self.dataset_index, item["dataset"], metadata)
        self._append_to_index(self.rse_index, item["rse"], metadata)
        self._append_to_index(self.scope_index, item["scope"], metadata)
        self._append_to_index(self.adler32_index, item["adler32"], metadata)
        self._append_to_index(self.timestamp_index, item["timestamp"], metadata)
        self._append_to_index(self.filenumber_index, item["filenumber"], metadata)
        self._append_to_index(self.location_index, item["location"], metadata)
        self._append_to_index(self.has_replicas_index, item["has_replicas"], metadata)

    def find_by_metadata(self, metadata_category, value):
        # Retrieve items by a specific metadata category and value
        index = getattr(self, f"{metadata_category}_index")
        return index.get(value)

    def _append_to_index(self, index, key, value):
        # Helper method to append metadata to an index (list) allowing for multiple values
        if key in index:
            index[key].append(value)
        else:
            index[key] = [value]
