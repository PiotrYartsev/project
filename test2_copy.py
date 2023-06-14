from rucio.client import Client

rucioclient = Client()

import json

#using the rucio API to get the list of scopes
def list_scopes():
    scopes=rucioclient.list_scopes()
    return(scopes)

def list_rses():
    rses=rucioclient.list_rses()
    return(rses)

#using the rucio API to get all the files in a rse
def list_files_dataset(scope, name):
    files = rucioclient.list_files(scope, name)
    return(files)
scopes=list_scopes()
#print(scopes)

rses=list_rses()
rses=list(rses)
rses_list=[]
for eachItem in rses:
    for key in eachItem:
        if key == "rse":
            #print(eachItem[key])
            rses_list.append(eachItem[key])

#read a txt line by line
with open('datasets_and_numbers.txt') as f:
    lines = f.readlines()
    
dataset=[]
for line in lines:
    #split str on ,
    line=line.split(",")
    line=line[0]
    dataset.append(line)

for dataset_name in dataset:
    scope, name = dataset_name.split(":")
    print(list(list_files_dataset(scope, name)))
    break
