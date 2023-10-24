import sqlite3 as sl
from rucio_functions import RucioFunctions
from multithreading import run_threads

class RucioDataset():
    #Check if the table for the dataset already exist
    @classmethod
    def check_if_exist(cls, dataset_and_scope,local_database_dataset_data):
        #retrive the dataset name and scope. FIXME This is a limitation of me setting up the multitrheading as a single function. Due to this I have to feed the changing arguments as a single object, that then is split into the subarguments. 
        dataset=dataset_and_scope[1]
        scope=dataset_and_scope[0]
        output=[]
        #We retrive the number of files in the dataset in Rucio. We then compare the number tot hte one in the local database. This is to see if there have been any changes.
        #FIXME Dont knwo how safe this is, can you move the files in Rucio? If so, this will not work, teh local database will say the files are stored at one location, while they are actually stored at another
        number_of_files_in_rucio=RucioFunctions.count_files_func(scope,dataset)
        
        #We check if the dataset is in the local database. If it is not, we add it to the local database
        for item in local_database_dataset_data:
            #set defoult value
            Found=False
            #check if the dataset is in the local database
            if item[1]==dataset and item[0]==scope:
                number_of_files_in_local=item[2]
                Found=True
                break
        #if the dataset is not in the local database, we return a False value, whic hmeans this dataset will be search for in Rucio
        if Found==False:
            output=[dataset_and_scope,False]
        else:
            #If the number of fiels match in Rucio and in the local database we return True, which means this dataset will not be searched for in Rucio
            #Else, we return False, which means this dataset will be searched for in Rucio
            if number_of_files_in_local==number_of_files_in_rucio:
                output=[dataset_and_scope,True]

            else:

                output=[dataset_and_scope,False]

        return output
    
    @classmethod
    def fill_data_from_local(cls, dataset_in_local):
        # Get a connection to the local database
        conn = sl.connect('local_rucio_database.db')

        #define some values
        dataset = dataset_in_local[1]
        scope = dataset_in_local[0]
        rse=dataset_in_local[2]
        

        # Query the local database for the table name for the given dataset
        query = "SELECT table_name FROM dataset WHERE scope=? AND name=?"
        table_name = conn.execute(query, (scope, dataset)).fetchone()[0]

        # Query the local database for all data for the given dataset. dont know why i split it into the 
        query = f"SELECT * FROM {table_name}"
        data_in_local_database = conn.execute(query).fetchall()
        
        #For GRIDFTP storage the location (addres/directory) of the files have the GRIDFTP address, whcih does not match the directory at the site. Therefore, we need to replace the GRIDFTP address with the local address
        #FIXME The values have been set manually here. It porbably should be in a config file, so it can be changed easily
        if rse =="LUND_GRIDFTP":     
            fix_with_this_string="/projects/hep/fs9/shared/ldmx/ldcs/gridftp/"
            replace_this_string="gsiftp://hep-fs.lunarc.lu.se:2811/ldcs/"
            data_in_local_database=[(item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7].replace(replace_this_string,fix_with_this_string),item[8]) for item in data_in_local_database]

        elif rse=="SLAC_GRIDFTP":
            fix_with_this_string=""
            replace_this_string="gsiftp://griddev01.slac.stanford.edu:2811"
            data_in_local_database=[(item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7].replace(replace_this_string,fix_with_this_string),item[8]) for item in data_in_local_database]
        
        #remove the file:// from the location
        data_in_local_database=[(item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7].replace("file://",""),item[8]) for item in data_in_local_database]
        
        # Create a list of FileMetadata objects from the data
        list_of_metadata = []
        #the location is just the completet path to the file, so if you remove the filename from the location, you get the directory
        for item in data_in_local_database:
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
        # Close the connection to the local database, dont know if this is needed
        conn.close()
        return list_of_metadata

        

    @classmethod
    def extract_from_rucio(cls,dataset,thread_count,rse):
        #define some values
        dataset_name=dataset[1]
        scope=dataset[0]
        #Rucio does not provide all the necessaty information abotu the files in a single place. Therefore, we need to use multiple functions to get all the information

        #First, we get the list of files in the dataset using the list_files_dataset function. It does not exist as part of the Rucio PythonAPI (for some reason even thought documentation says it does), so we use the CLI version instead instead
        #FIXME Make this work with python API, we dont want two systems
        files_using_list_dataset_replicas_bulk_CLI=((RucioFunctions.list_dataset_replicas_bulk_CLI(scope=scope,name=dataset_name,rse=rse)))
        datastructure=[]
        #If the dataset is empty, we return an empty datastructure
        if len(files_using_list_dataset_replicas_bulk_CLI)==0:
            return datastructure
        else:
            #We use multithreading to process the data from Rucio. We do this as it is faster than doing it in a single thread
            output=run_threads(thread_count=thread_count,function=RucioDataset.multithreaded_add_to_FileMetadata,data=files_using_list_dataset_replicas_bulk_CLI,const_data=dataset)
            print(output[0].name,output[0].rse)
            datastructure.extend(output)
            
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
        #We just define the values we got from Rucio
        dataset_name=scope_dataset[1]
        scope=file[0]
        name=file[1]
        adler32=file[3]
        rse=file[4].split(":")[0]
        location=file[4].replace(rse+":","")

        #For GRIDFTP storage the location (addres/directory) of the files have the GRIDFTP address, whcih does not match the directory at the site. Therefore, we need to replace the GRIDFTP address with the local address
        #FIXME The values have been set manually here. It porbably should be in a config file, so it can be changed easily
        if rse =="LUND_GRIDFTP":
                fix_with_this_string="/projects/hep/fs9/shared/ldmx/ldcs/gridftp/"
                replace_this_string="gsiftp://hep-fs.lunarc.lu.se:2811/ldcs/"
                location=location.replace(replace_this_string,fix_with_this_string)

        elif rse=="SLAC_GRIDFTP":
                fix_with_this_string=""
                replace_this_string="gsiftp://griddev01.slac.stanford.edu:2811"
                location=location.replace(replace_this_string,fix_with_this_string)

        #remove the file:// from the location
        location=location.replace("file://","")
        #extract the timestamp and filenumber from the name
        timestamp=name.split("_")[-1].replace(".root","").replace("t","")
        filenumber=name.split("_")[-2].replace("run","")
        #set has_replicas to 0, as we dont know if it has replicas yet. FIXME implemetn proper check
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
    
    # Add multiple class FileMetadata to the data structure at once
    def multiadd(self, items):
        # Get the current maximum id. we use this to assign a new id to the new items
        current_max_id = max(self.id_index.keys()) if self.id_index else -1
        # Create a list of new ids
        new_ids = range(current_max_id + 1, current_max_id + len(items) + 1)

        # Create a list of FileMetadata instances
        metadata_list = [
            FileMetadata(
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
            for item, new_id in zip(items, new_ids)
        ]

        # Append the metadata to the relevant indexes, using lists
        for metadata in (metadata_list):
            self._append_to_index(self.name_index, metadata.name, metadata)
            self._append_to_index(self.dataset_index, metadata.dataset, metadata)
            self._append_to_index(self.scope_index, metadata.scope, metadata)
            self._append_to_index(self.rse_index, metadata.rse, metadata)
            self._append_to_index(self.adler32_index, metadata.adler32, metadata)
            self._append_to_index(self.timestamp_index, metadata.timestamp, metadata)
            self._append_to_index(self.filenumber_index, metadata.filenumber, metadata)
            self._append_to_index(self.location_index, metadata.location, metadata)
            self._append_to_index(self.directory_index, metadata.directory, metadata)
            self._append_to_index(self.has_replicas_index, metadata.has_replicas, metadata)
            self._append_to_index(self.id_index, metadata.id, metadata)


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