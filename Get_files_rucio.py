import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from DDS_comparison_tool import *
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl

#initialise default values
tqmdis=False
comments=False
limit=0
checksum=True
All=False


#gets all the valid scopes registered in Rucio
def list_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   for n in range(len(scopes)):
       scopes[n]=scopes[n].replace("\n","")
   return(scopes)


#For a scope get all datasets registered in rucio for that scope 1.
def get_all_datasets(scopes):
    All_datasets=[]
    #Iterate over all scopes, run function get_datasets_from_scope from scope
    for scope in scopes:
        datasets_scope=get_datasets_from_scope(scope)
        All_datasets=list(set(All_datasets+datasets_scope))
    return(All_datasets)


#For a scope get all datasets registered in rucio for that scope 2.
def get_datasets_from_scope(scope):
    #Rucio CLI command returns the datasets as a str
    datasets_scope=list(os.popen("rucio list-dids --filter type=DATASET {}:*".format(scope)))
    #Removes rows not containing data
    [ x for x in datasets_scope if "|" in x ]
    #Clean the data a bit
    for n in range(len(datasets_scope)):
        datasets_scope[n]=datasets_scope[n].replace("| DATASET      |\n'","")
        datasets_scope[n]=datasets_scope[n].replace("|","")
        datasets_scope[n]=datasets_scope[n].replace(" ","")
        datasets_scope[n]=datasets_scope[n].replace("\n","")
        datasets_scope[n]=datasets_scope[n].replace("DATASET","")
    return(datasets_scope)

#List all registered rse in Rucio
def list_rse():
    rses=list(os.popen("rucio list-rses"))
    for n in range(len(rses)):
        rses[n]=rses[n].replace("\n","")
    return(rses)


#For every dataset provided it gets all file replicas that are located at a particular rse
def files_from_datasets(datasets, rses):

    #remove duplicate datasets
    datasets=list(set(datasets))
    datasets=[file for file in datasets if not "----------" in file]

    #print if output is turned on
    if comments==True:
        print("\nGet a list of all the files in a dataset.")
    
    #library storing all the files found at a rse where keys are rse and items are a list of files
    datasets_rse={}


    for rse in rses:
        #list of files found for the rse
        list_of_files=[]

        #lsit to keep track of how many files are located at a rse for a particular dataset. This info can be used in the future to classify problematic files  
        number_of_files_in_dataset={}

        #"if limit==0" is to check if you want to run the version for all the datasets or only for some of them.
        if limit==0:
            for n in tqdm(range(len(datasets)), disable=tqmdis):
                dataset=datasets[n]
                #Clean the datasets by removing the first and last line that contains junk
                if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
                    pass
                else:
                    #Get all the files in a dataset at a rse and save it as a list by splitting at each line
                    L=(os.popen("rucio list-file-replicas --rses {} {}".format(rse,dataset)).read()).split('\n')

                    #Clean the data a bit by removing any line not containing the name of the rse, which is present in every file location.
                    L=[file for file in L if rse in file]
                    L=[file+str(dataset) for file in L]

                    #add the number of files with a particular rse to the dictionary
                    number_of_files_in_dataset[dataset]=len(L)

                    #files to file list
                    list_of_files.extend(L)
        else:
            #Same as above
            if comments==True:
                print("Limit set to {}\n".format(limit))
            for n in tqdm(range(len(datasets[:limit])), disable=tqmdis):
                dataset=datasets[n]
                if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
                    pass
                else:
                    L=(os.popen("rucio list-file-replicas --rses {} {}".format(rse,dataset)).read()).split('\n')
                    L=[file for file in L if rse in file]
                    L=[file+str(dataset) for file in L]
                    number_of_files_in_dataset[dataset]=len(L)
                    list_of_files.extend(L)
        #Remove any empty datasets from the dataset number dictionary            
        number_of_files_in_dataset = dict( [(k,v) for k,v in number_of_files_in_dataset.items() if not v==0])

        #Remove duplicate files from the file list
        list_of_files=list(set(list_of_files))
        
        #Turn each file entry into a list by splitting on |
        new_new_list=[file.replace(" ","").split("|") for file in list_of_files]
        #Removing the first two entries that don't contain important information
        new_new_list=[file[2:] for file in new_new_list]
        
        #for the rse add all file list to the dictionary
        datasets_rse[rse]=new_new_list
        
        #Print statistics for a rse
        if comments==True:
            print("\n")
            print("For the rse {} we have this many files:".format(rse))
            print(len(datasets_rse[rse]))

    #If no files were found for rse remove it from dictionary
    datasets_rse = dict( [(k,v) for k,v in datasets_rse.items() if len(v)>0])
    return(datasets_rse, number_of_files_in_dataset)

#clean the data by removing useless information and remove the filename from the storage path
def clean_up_datasets_rse(datasets_rse):
    for rse in datasets_rse:
        if "GRIDFTP" in rse:
            grid_settings=open("config.txt","r")
            grid_settings_list=grid_settings.readlines()
            for line in grid_settings_list:
                if rse in line:
                    
                    address_to_change=line.split(",")[1]
                    change_with=line.split(',')[2]
                    change_with=change_with.replace('\n',"")
                   
            if comments==True:
                print("\nCleaning up data about files at {}".format(rse))

            dataset_list=datasets_rse[rse]
            #print(dataset_list[:2])
            for n in tqdm(range(len(dataset_list)), disable=tqmdis):
                dataset=dataset_list[n]
                
                dataset[3]=dataset[3].replace(dataset[0],"")
                
                dataset[3]=dataset[3].replace(address_to_change,change_with)
                
                
                
        else:
            if comments==True:
                print("\nCleaning up data about files at {}".format(rse))

            dataset_list=datasets_rse[rse]
            #print(dataset_list[10:])
            for n in tqdm(range(len(dataset_list)), disable=tqmdis):
                dataset=dataset_list[n]
                dataset[3]=dataset[3].replace(dataset[0],"")
                dataset[3]=dataset[3].replace("{}:file://".format(rse),"")
    return (datasets_rse)
    
import time as t
import datetime
#today's date
today = datetime.date.today()
today=today.strftime("%d-%m-%Y")

con = sl.connect('rucio_{}.db'.format(today))
dataset="dataset"

with con:
        con.execute("""
            CREATE TABLE {} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                file TEXT,
                BatchID TEXT,
                ComputingElement TEXT,
                DataLocation TEXT,
                Scope TEXT,
                JobSubmissionTime INTEGER,
                FileCreationTime INTEGER
            );
        """.format(dataset))

sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime) values(?, ?, ?, ?, ?, ?, ?, ?)'.format(dataset)