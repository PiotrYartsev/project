#this is the main software that runs all the other parts
import Rucio_functions as RucioFunctions

#from archives_generator import archives_generator,move_to_archives

#from generate_gridftp_instructional_files import adding_data_about_replicas

#from generate_text_rse_docs import generate_txt

#from read_rse_output_and_compare_to_rucio import find_dark_data

#from dark_data_functions import transforming_to_database_from_txt

from argument_loader import get_args

from verify import verify
import datetime
import shutil
import logging
"""

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler and set its level to INFO
file_handler = logging.FileHandler('myapp.log')
file_handler.setLevel(logging.INFO)

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


#Get the width of the terminal and print a welcome message
terminal_width, _ = shutil.get_terminal_size()


#Get todays date, which is used for naming archives and finding the most resent storage dump
today=datetime.date.today()
today=str(today)
today=today.replace("-","_")


archives_generator(today,logger)


adding_data_about_replicas(logger)


generate_txt(logger)


transforming_to_database_from_txt(today,logger)


find_dark_data(logger)


verify(logger)


move_to_archives(today,logger)
"""

from rucio.client import Client
from urllib.parse import quote_plus
import subprocess as subprocess
import os 

class RucioFunctions:
    rucioclient = Client()

    @classmethod
    def list_scopes(cls):
        try:
            scopes = cls.rucioclient.list_scopes()
            return scopes
        except Exception as e:
            print(f"Error listing scopes: {e}")
            return None

    @classmethod
    def list_rses(cls):
        try:
            rses = cls.rucioclient.list_rses()
            return rses
        except Exception as e:
            print(f"Error listing RSEs: {e}")
            return None

    @classmethod
    def list_files_dataset(cls, scope, name):
        try:
            files = cls.rucioclient.list_files(scope=scope, name=name, long=True)
            return files
        except Exception as e:
            print(f"Error listing files in dataset: {e}")
            return None

    @classmethod
    def get_rse_usage(cls, rse):
        try:
            rse_info = cls.rucioclient.get_rse_usage(rse)
            return rse_info
        except Exception as e:
            print(f"Error getting RSE usage: {e}")
            return None

    @classmethod
    def get_metadata(cls, scope, name):
        try:
            metadata = cls.rucioclient.get_metadata(scope, name)
            return metadata
        except Exception as e:
            print(f"Error getting metadata: {e}")
            return None

    @classmethod
    def list_replicas(cls, scope, name):
        try:
            replicas = cls.rucioclient.list_replicas(dids=[{'scope': scope, 'name': name}])
            return replicas
        except Exception as e:
            print(f"Error listing replicas: {e}")
            return None

    @classmethod
    def list_replicas_batch(cls, file_list):
        # Create a list of DIDs from the input file list
        dids = [{'scope': scope, 'name': name} for scope, name in file_list]
        try:
            # Use the list_replicas method to check for the existence of the files
            replicas = cls.rucioclient.list_replicas(dids=dids)
            replicas = list(replicas)
            return replicas
        except Exception as e:
            print(f"Error listing replicas in bulk: {e}")
            return None

    @classmethod
    def count_files_func(cls, scope, dataset_name):
        try:
            dataset_info = cls.rucioclient.list_dataset_replicas(scope, dataset_name, deep=True)
            length = 0
            dataset_info = list(dataset_info)
            for each in dataset_info:
                length = length + each["available_length"]
            return length, dataset_info
        except Exception as e:
            print(f"Error counting files in dataset: {e}")
            return None

    @classmethod
    def check_files_exist(cls, file_list):
        # Create a list of DIDs from the input file list
        dids = [{'scope': scope, 'name': name} for scope, name in file_list]

        try:
            # Use the list_replicas method to check for the existence of the files
            replicas = cls.rucioclient.list_replicas(dids=dids)
            results = []
            replicas = list(replicas)

            # Iterate over the list of replicas and append the (scope, name) tuples to the results list
            for replica in replicas:
                scope = replica['scope']
                name = replica['name']
                results.append((scope, name))

            return results
        except Exception as e:
            print(f"Error checking file existence: {e}")
            return None

    @classmethod
    def list_dataset(cls, scope):
        # Run the CLI command "rucio list-dids --filter type=DATASET scope:*"
        command = f"rucio list-dids --filter type=DATASET {scope}:*"
        try:
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
            dataset = result.stdout.strip().split("\n")[3:-2]
            dataset = [x.strip().replace("DATASET", "").replace(scope+":", "").replace("|", "").replace(" ", "") for x in dataset]
            return dataset
        except Exception as e:
            print(f"Error listing datasets: {e}")
            return None



if __name__ == "__main__":
    args = get_args()
    print(args)
    if args.all:
        print("all")
        #run the code for all of Rucio at the same time
    elif args.scopes:
        print("scopes")
        #first, retrive a list of all the scopes, to see if the scopes are valid
        scopes_in_rucio = RucioFunctions.list_scopes()
        print(scopes_in_rucio)
        scopes = args.scopes
        print(scopes)
        for scope in scopes:
            if scope not in scopes_in_rucio:
                print(f"Error: scope {scope} is not valid")
                exit(1)
        #find all the datasets in the scopes
        #then find all the files in the datasets
    elif args.datasets:
        print("datasets")
        #first, retrive a list of all the datasets, to see if the datasets are valid
        datasets = args.datasets
        scopes_in_rucio = RucioFunctions.list_scopes()
        datasets_in_rucio= RucioFunctions.list_datasets(scope=scopes_in_rucio)
        for dataset in datasets:
            if dataset not in datasets_in_rucio:
                print(f"Error: dataset {dataset} is not valid")
                exit(1)
        
        #find all the files in the datasets
        #
    else:
        print("Error: no scopes or datasets specified")
        exit(1)
