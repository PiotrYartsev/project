from test_func import *
import sys

if __name__ == '__main__':
    

#what do I want to run
#current bypas for the problems with rucio list-scopes, rucio list-dids --filter type=DATASET, rucio list-datasets-rse
datasets=["mc20:v2.2.1-3e","mc20:v2.2.1-3e","mc20:v9-8GeV-1e-inclusive"]

#Get all the files in the dataset
L2=files_from_datasets(datasets, "LUND")

#get information about the files in storage, such as their name and 
#get_info_from_all_data_storage2("LUND","/projects/hep/fs7/scratch/pflorido/ldmx-pilot/pilotoutput/ldmx/mc-data/v9/8.0GeV/")

#compare the files from rucio with the files in storage
check_if_the_file_exist_bash(L2)

#check_if_the_file_exist_python(L2)
#python is just broken


a="/home/pioyar/Desktop/project/output/mc20:v2.2.1-3e/not_missing/not_missing_2022-03-3115:29:41.306890.txt"
compere_checksum(a)
#print(get_adler32_checksum("/projects/hep/fs9/shared/ldmx/ldcs/output/ldmx/mc-data/mc20/v12/4.0GeV/v2.2.1-3e/mc_v12-4GeV-3e-inclusive_run1310171_t1601590919.root"))
