from rucio.client import Client
from urllib.parse import quote_plus
#run Cli from python
import subprocess as subprocess
rucioclient = Client()

#using the rucio API to get the list of scopes
def list_scopes():
    scopes=rucioclient.list_scopes()
    return(scopes)


#using the rucio API to get the list of rses
def list_rses():
    rses=rucioclient.list_rses()
    return(rses)

#using the rucio API to get all the files in a rdataset
def list_files_dataset(scope, name):
    files=rucioclient.list_files(scope=scope, name=name, long=True)
    return(files)


#use the rucio API to get the statistics of rse usage
def get_rse_usage(rse):
    rse_info=rucioclient.get_rse_usage(rse)
    return(rse_info)

#use the rucio API to get metadata in bulk
def get_metadata(scope, name):
    metadata=rucioclient.get_metadata(scope, name)


#use the rucio API to get the list of replicas fo a file, which gives us the where the file is stored 
def list_replicas(scope, name):
    replicas=rucioclient.list_replicas(dids=[{'scope':scope, 'name':name}])
    return(replicas)

def count_files_func(scope,dataset_name):
    dataset_info=rucioclient.list_dataset_replicas(scope,dataset_name,deep=True)
    lenght=0
    dataset_info=list(dataset_info)
    for each in dataset_info:
        lenght=lenght+each["available_length"]
    return(lenght,dataset_info)



def check_file_exists(scope, name):
    replicas = rucioclient.list_replicas(dids=[{'scope': scope, 'name': name}])
    if not replicas:
        return False
    else:
        return True
    

def list_dataset(scope):
    # run the CLI command rucio list-dids --filter type=DATASET scope:*
    command = f"rucio list-dids --filter type=DATASET {scope}:*"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
    dataset = result.stdout.strip().split("\n")[3:-2]
    dataset = [x.strip().replace("DATASET", "").replace(scope+":", "").replace("|", "").replace(" ", "") for x in dataset]
    return dataset

