from Rucio_functions import *
import sys
import os
from tqdm import tqdm


"""
scopes=list_scopes()

rses = list_rses()
rses_list = []
for rse in rses:
    rse_name = rse['rse']
    rses_list.append(rse_name)
"""

#read a txt line by line to extract the datasets
with open('datasets_and_numbers.txt') as f:
    lines = f.readlines()

#remove whitespace characters like `\n` at the end of each line and remove the count of files
dataset=[]
for line in lines:
    #split str on ,
    line=line.split(",")
    #remove the count of files
    line=line[0]
    dataset.append(line)

data=[]
for dataset_name in tqdm(dataset):
    scope, name = dataset_name.split(":")
    output=(list(list_files_dataset(scope, name)))
    for i in tqdm(output):
        #print(output)
        file_info={}
        file_info["scope"]=i["scope"]
        file_info["name"]=i["name"]
        file_info["adler32"]=i["adler32"]
        #get metadatata
        ##print("getting metadata")
        #print(scope, ": ", output["name"])
        metadata=list_replicas(scope, i["name"])
        metadata=list(metadata)[0]
        #print(metadata["rses"])
        location={}
        if len (metadata["rses"])>1:
            for key in metadata["rses"]:
                #print(key)
                file_info["rse"]=key
                
                location[key]=metadata["rses"][key]
                

                #print(file_info)

                
                break
            file_info["location"]=location
            print(file_info)
            break
        else:
            pass
    
    
#"""