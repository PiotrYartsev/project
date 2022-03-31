from test_func import *
import sys


#what do I want to run

#current bypas for the problems with rucio list-scopes, rucio list-dids --filter type=DATASET, rucio list-datasets-rse
datasets=[]
for n in range(1):
    #datasets.append("mc20:v9-8GeV-1e-inclusive")
    datasets.append("mc20:v2.2.1-3e")

#Get all the files in the dataset
L2=files_from_datasets(datasets)

#get information about the files in storage, such as their name and 
#get_info_from_all_data_storage2("LUND","/projects/hep/fs7/scratch/pflorido/ldmx-pilot/pilotoutput/ldmx/mc-data/v9/8.0GeV/")

#compare the files from rucio with the files in storage

#check_if_the_file_exist_python(L2)

a=check_if_the_file_exist_bash(L2)
#compere_checksum(a[0])