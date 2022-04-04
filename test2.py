import os
from zlib import adler32
from datetime import datetime
from tqdm import *


tqmdis=False
comments=False
limit=0


def list_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   for n in range(len(scopes)):
       scopes[n]=scopes[n].replace("\n","")
   return(scopes)


def get_datasets_from_scope(scope):
    datasets_scope=list(os.popen("rucio list-dids --filter type=DATASET {}:*".format(scope)))
    [ x for x in datasets_scope if "|" in x ]
    for n in range(len(datasets_scope)):
        datasets_scope[n]=datasets_scope[n].replace("| DATASET      |\n'","")
        datasets_scope[n]=datasets_scope[n].replace("|","")
        datasets_scope[n]=datasets_scope[n].replace(" ","")
        datasets_scope[n]=datasets_scope[n].replace("\n","")
        datasets_scope[n]=datasets_scope[n].replace("DATASET","")
    return(datasets_scope)


def get_all_datasets(scopes):
    All_datasets=[]
    for scope in scopes:
        datasets_scope=get_datasets_from_scope(scope)
        All_datasets=list(set(All_datasets+datasets_scope))
    return(All_datasets)



def list_rse():
    rses=list(os.popen("rucio list-rses"))
    for n in range(len(rses)):
        rses[n]=rses[n].replace("\n","")
    return(rses)

from run_test import *

def files_from_datasets(datasets, rses):
    if comments==True:
        print("Get a list of all the files in a dataset.")
    list_of_files=[]
    for n in tqdm(range(len(datasets[:4])), disable=tqmdis):
        dataset=datasets[n]
        L=(os.popen("rucio list-file-replicas {}".format(dataset)).read()).split('\n')
        [file+dataset for file in L]
        list_of_files.extend(L)
    datasets_rse={}

    for rse in rses:
        if "GRIDFTP" in rse:
            newlist=[file for file in list_of_files if rse in file]
        else:
            newlist=[file for file in list_of_files if rse in file and "GRIDFTP" not in file]
        new_new_list=[file.replace(" ","").split('|') for file in newlist]
        new_new_list=[file[2:] for file in new_new_list]
        datasets_rse[rse]=new_new_list
        if comments==True:
            print("\n")
            print("For the rse {} we have this many files:".format(rse))
            print(len(newlist))

    datasets_rse = dict( [(k,v) for k,v in datasets_rse.items() if len(v)>0])

    return(datasets_rse)
        
def clean_up_datasets_rse(datasets_rse):
    for rse in datasets_rse:
        if comments==True:
            print("\nCleaning up data about files at {}".format(rse))
        dataset_list=datasets_rse[rse]
        for n in tqdm(range(len(dataset_list))):
            dataset=dataset_list[n]
            dataset[3]=dataset[3].replace(dataset[0],"")
            dataset[3]=dataset[3].replace("{}:file://".format(rse),"")
    return(datasets_rse)
    

#For a provided file file2 and directory dir2 it will return the adler32 shecksum in hexadecimal
def get_adler32_checksum(file2):
    BLOCKSIZE=256*1024*1024
    asum=1
    try:
        with open("{}".format(file2),"rb") as f:
            while True:
                data = f.read(BLOCKSIZE)
                if not data:
                    break
                asum = adler32(data, asum)
                if asum < 0:
                    asum += 2**32
    except:
        asum=None
    return(asum)



#compares the adler34 checksum for the files we matched between storage and rucio. Outputs the rucio entries without a match, the checksum of the file in storage, the checksum as reported by rucio.
def compere_checksum(datasets_rse):
    
    for rse in datasets_rse:
        files_found={}
        files_not_found={}
        integ=0
        if comments==True:
            print("\nComparing the adler32 checksum in rucio with checksum in storage for rse {}.".format(rse))
        datasets=datasets_rse[rse]
        for n in tqdm(range(len(datasets)), disable=tqmdis):
            file_data=datasets[n]

            file=file_data[0]
            
            directory=file_data[3]

            fullpath=directory+file
            
            checksum_rucio=file_data[2]

            failed_adles32="adler32_fail"+file+".txt"

            checksum_dec=get_adler32_checksum(fullpath)

            batch=file_data[-1]
            print(batch)
            if checksum_dec==None:
                if batch in files_not_found:
                    files_not_found[batch].append([file,directory]) 
                else:   
                    files_not_found[batch]=[]
                    files_not_found[batch].append([file,directory]) 
            else:
                if batch in files_found:
                    files_found[batch].append([file,directory]) 
                else:   
                    files_found[batch]=[]
                    files_found[batch].append([file,directory])
                checksum_hex=hex(checksum_dec)
                checksum_hex=checksum_hex.lstrip("0x").rstrip("L")
                
                
                if str(checksum_hex)==checksum_rucio:
                    pass
                else:
                    integ=integ+1
                    os.system("echo {},{},{} >> adler32.txt".format(file,checksum_rucio,str(checksum_hex)))
                    #print(new_error_file)
        number_failed_files=0
        number_sucesfull_files=0
        for batch in files_not_found:
            number_failed_files=number_failed_files+len(files_not_found[batch])
        
        for batch in files_found:
            number_sucesfull_files=number_sucesfull_files+len(files_found[batch])

        if comments==True:    
            print(number_failed_files)
            print(number_sucesfull_files)

            print("\nFor the rse {} we found that {} files were missing out of {}.".format(rse, number_failed_files, len(datasets)))
            print("We found {} corrupted files out of {}".format(integ,(len(datasets))))
        os.system("echo {} >> files_not_found.txt".format(files_not_found,files_found,))
        os.system("echo {} >> files_found.txt".format(files_found,))
