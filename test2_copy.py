from rucio.client import Client

rucioclient = Client()

import json

#using the rucio API to get the list of scopes
def list_scopes():
    scopes=rucioclient.list_scopes()
    return(scopes)

def list_rses():
    rses=rucioclient.list_rses()
    rses = json.loads(rses)
    return(rses)

#using the rucio API to get all the files in a rse
def list_files_rse(rse_name):
    try:
        files=rucioclient.list_replicas([rse_name])
        return(files)
    except Exception as e:
        print(f"Error: {e}")
        return []

scopes=list_scopes()
#print(scopes)

rses=list_rses()
print(rses)

rses=list(rses)

# prompt the user to enter the RSE name
rse_name = input("Enter the RSE name: ")

files=list_files_rse(rse_name)

if files:
    print(files)
else:
    print(f"No files found in RSE {rse_name}")