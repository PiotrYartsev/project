from rucio.client import Client
from urllib.parse import quote_plus
import subprocess as subprocess
import os 

# Create a Rucio client object
rucioclient = Client()

# Function to list all the scopes in Rucio
def list_scopes():
    try:
        scopes = rucioclient.list_scopes()
        return scopes
    except Exception as e:
        print(f"Error listing scopes: {e}")
        return None

# Function to list all the RSEs in Rucio
def list_rses():
    try:
        rses = rucioclient.list_rses()
        return rses
    except Exception as e:
        print(f"Error listing RSEs: {e}")
        return None

# Function to list all the files in a dataset
def list_files_dataset(scope, name):
    try:
        files = rucioclient.list_files(scope=scope, name=name, long=True)
        return files
    except Exception as e:
        print(f"Error listing files in dataset: {e}")
        return None

# Function to get the usage statistics of an RSE
def get_rse_usage(rse):
    try:
        rse_info = rucioclient.get_rse_usage(rse)
        return rse_info
    except Exception as e:
        print(f"Error getting RSE usage: {e}")
        return None

# Function to get metadata for a file
def get_metadata(scope, name):
    try:
        metadata = rucioclient.get_metadata(scope, name)
        return metadata
    except Exception as e:
        print(f"Error getting metadata: {e}")
        return None

# Function to list all the replicas of a file
def list_replicas(scope, name):
    try:
        replicas = rucioclient.list_replicas(dids=[{'scope': scope, 'name': name}])
        return replicas
    except Exception as e:
        print(f"Error listing replicas: {e}")
        return None

# Function to list all the replicas for a list of files
def list_replicas_batch(file_list):
    # Create a list of DIDs from the input file list
    dids = [{'scope': scope, 'name': name} for scope, name in file_list]
    try:
        # Use the list_replicas method to check for the existence of the files
        replicas = rucioclient.list_replicas(dids=dids)
        replicas = list(replicas)
        return replicas
    except Exception as e:
        print(f"Error listing replicas in bulk: {e}")
        return None
    


# Function to count the number of files in a dataset
def count_files_func(scope, dataset_name):
    try:
        dataset_info = rucioclient.list_dataset_replicas(scope, dataset_name, deep=True)
        length = 0
        dataset_info = list(dataset_info)
        for each in dataset_info:
            length = length + each["available_length"]
        return length, dataset_info
    except Exception as e:
        print(f"Error counting files in dataset: {e}")
        return None

# Function to check if a list of files exist in Rucio
def check_files_exist(file_list):
    # Create a list of DIDs from the input file list
    dids = [{'scope': scope, 'name': name} for scope, name in file_list]

    try:
        # Use the list_replicas method to check for the existence of the files
        replicas = rucioclient.list_replicas(dids=dids)
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

# Function to list all the datasets in a scope
def list_dataset(scope):
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