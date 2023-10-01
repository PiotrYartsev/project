import sqlite3 as sl
from tqdm import tqdm
import os
from Rucio_functions import RucioFunctions
from multithreading import run_threads

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
                    directory=item[7].replace("/"+item[1],""),
                    has_replicas=item[8]
                )
                list_of_metadata.append(metadata)

            # Return the custom data structure
            return list_of_metadata

    @classmethod
    def extract_from_rucio(cls,dataset,thread_count):
        dataset_name=dataset[1]
        scope=dataset[0]
        #Rucio does not provide all the necessaty information abotu the files in a single place. Therefore, we need to use multiple functions to get all the information

        #First, we get the list of files in the dataset using the list_files_dataset function. It does not exist as part of the Rucio PythonAPI (for some reason even thought documentation says it does), so we use the CLI version instead instead
        #FIXME Make this work with python API, we dont want two systems
        files_using_list_dataset_replicas_bulk_CLI=((RucioFunctions.list_dataset_replicas_bulk_CLI(scope=scope,name=dataset_name)))
        datastructure=CustomDataStructure()
        if len(files_using_list_dataset_replicas_bulk_CLI)==0:
            return datastructure
        else:
            output=run_threads(thread_count=thread_count,function=RucioDataset.multithreaded_add_to_FileMetadata,data=files_using_list_dataset_replicas_bulk_CLI,const_data=dataset)
            for item in output:
                datastructure.add_item(item)
            return datastructure
    
    @classmethod
    def find_replicas(cls, datastructure,rse):
        #for each file in the datastructure, we check if it has replicas
        for metadata in datastructure.name_index.values():
            # Do something with the metadata instance
            metadata=metadata[0]
            dict_search={"name":metadata.name,"scope":metadata.scope,"adler32":metadata.adler32}
            matching_index=datastructure.find_by_metadata_dict(dict_search)
            if len(matching_index)!=1:
                for item in matching_index:
                    item.has_replicas=1

    def multithreaded_add_to_FileMetadata(file, scope_dataset):
        dataset_name=scope_dataset[1]
        scope=file[0]
        name=file[1]
        adler32=file[3]

        rse=file[4].split(":")[0]
        location=file[4].replace(rse+":","")
        timestamp=name.split("_")[-1].replace(".root","").replace("t","")
        filenumber=name.split("_")[-2].replace("run","")
        has_replicas=0

        #check if a file with this name and scope and adler32 already exist in the datastructure
        #if it does, then we set has_replcias to 1 for both
        #if it does not, we set has_replicas to 0 for both
        metadata=FileMetadata(
            name=name,
            dataset=dataset_name,
            scope=scope,
            rse=rse,
            adler32=adler32,
            timestamp=timestamp,
            filenumber=filenumber,
            location=location,
            directory=location.replace("/"+name,""),
            has_replicas=has_replicas
        )
        return metadata
    
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
    def __init__(self, name, dataset, scope, rse, adler32, timestamp, filenumber, location,directory, has_replicas,id=None):
        self.name = name
        self.dataset = dataset
        self.scope = scope
        self.rse = rse
        self.adler32 = adler32
        self.timestamp = timestamp
        self.filenumber = filenumber
        self.location = location
        self.directory = directory
        self.has_replicas = has_replicas
        self.id = id

    def __getitem__(self, key):
        return getattr(self, key)
        
class CustomDataStructure:
    def __init__(self):
        # Initialize dictionaries (indexes) for each metadata category
        self.name_index = {}
        self.dataset_index = {}
        self.scope_index = {}
        self.rse_index = {}
        
        self.adler32_index = {}
        self.timestamp_index = {}
        self.filenumber_index = {}
        self.location_index = {}
        self.directory_index = {}
        self.has_replicas_index = {}
        self.id_index = {}

    def add_item(self, item):
        current_max_id = max(self.id_index.keys()) if self.id_index else -1
        new_id = current_max_id + 1

        # Create a FileMetadata instance
        metadata = FileMetadata(
            item["name"],
            item["dataset"],
            item["scope"],
            item["rse"],
            
            item["adler32"],
            item["timestamp"],
            item["filenumber"],
            item["location"],
            item["directory"],
            item["has_replicas"],
            id=new_id
        )
        
        # Append the metadata to the relevant indexes, using lists
        self._append_to_index(self.name_index, item["name"], metadata)
        self._append_to_index(self.dataset_index, item["dataset"], metadata)
        self._append_to_index(self.scope_index, item["scope"], metadata)
        self._append_to_index(self.rse_index, item["rse"], metadata)
        
        self._append_to_index(self.adler32_index, item["adler32"], metadata)
        self._append_to_index(self.timestamp_index, item["timestamp"], metadata)
        self._append_to_index(self.filenumber_index, item["filenumber"], metadata)
        self._append_to_index(self.location_index, item["location"], metadata)
        self._append_to_index(self.directory_index, item["directory"], metadata)
        self._append_to_index(self.has_replicas_index, item["has_replicas"], metadata)
        self._append_to_index(self.id_index, new_id, metadata)

    def find_by_metadata(self, metadata_category, value):
        # Retrieve items by a specific metadata category and value
        index = getattr(self, f"{metadata_category}_index")
        return index.get(value)
    
    def find_by_metadata_dict(self, metadata_dict):
        # Retrieve items by multiple metadata categories and values
        indexes = []
        for category, value in metadata_dict.items():
            index = getattr(self, f"{category}_index")
            indexes.append(set(index.get(value, [])))
        result = set.intersection(*indexes)
        return list(result)

    def _append_to_index(self, index, key, value):
        # Helper method to append metadata to an index (list) allowing for multiple values
        if key in index:
            index[key].append(value)
        else:
            index[key] = [value]

def combine_datastructures(datastructure1, datastructure2):
    # Create a new CustomDataStructure instance
    combined_datastructure = CustomDataStructure()
    print("Combining datastructures")
    # Add all the items from datastructure1 to the combined_datastructure
    for item in datastructure1.id_index.values():
        #print id
        for metadata in item:
            combined_datastructure.add_item({
                "name": metadata.name,
                "dataset": metadata.dataset,
                "scope": metadata.scope,
                "rse": metadata.rse,
                "adler32": metadata.adler32,
                "timestamp": metadata.timestamp,
                "filenumber": metadata.filenumber,
                "location": metadata.location,
                "directory": metadata.directory,
                "has_replicas": metadata.has_replicas
            })
    print("Number of items in datastructure1:", len(datastructure1.id_index))
    print("Number of items added to combined_datastructure from datastructure1:", len(combined_datastructure.id_index))
    
    # Add all the items from datastructure2 to the combined_datastructure
    for item in datastructure2.name_index.values():
        for metadata in item:
            combined_datastructure.add_item({
                "name": metadata.name,
                "dataset": metadata.dataset,
                "scope": metadata.scope,
                "rse": metadata.rse,
                "adler32": metadata.adler32,
                "timestamp": metadata.timestamp,
                "filenumber": metadata.filenumber,
                "location": metadata.location,
                "directory": metadata.directory,
                "has_replicas": metadata.has_replicas
            })
    print("Number of items in datastructure2:", len(datastructure2.name_index))
    print("Number of items added to combined_datastructure from datastructure2:", len(combined_datastructure.id_index))
    
    return combined_datastructure
