
from DDS_comparison_tool import *
from rucio.client import Client
rucioclient = Client()
#using the rucio API to get the list of scopes
def list_scopes():
    scopes=rucioclient.list_scopes()
    return(scopes)
#using the rucio API to get the list of datasets
def get_all_datasets(scopes):
    All_datasets=[]
    #Iterate over all scopes, run function get_datasets_from_scope from scope
    for scope in scopes:
        datasets_scope=rucioclient.list_files(scope)
    return datasets_scope
    
scopes=list_scopes()
datasets=get_all_datasets(scopes)