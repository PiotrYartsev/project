import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from DDS_comparison_tool import *

tqmdis=False
comments=False
limit=0
checksum=True


def list_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   for n in range(len(scopes)):
       scopes[n]=scopes[n].replace("\n","")
   return(scopes)

def get_all_datasets(scopes):
    All_datasets=[]
    for scope in scopes:
        datasets_scope=get_datasets_from_scope(scope)
        All_datasets=list(set(All_datasets+datasets_scope))
    return(All_datasets)

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


def list_rse():0
    rses=list(os.popen("rucio list-rses"))
    for n in range(len(rses)):
        rses[n]=rses[n].replace("\n","")
    return(rses)



def files_from_datasets(datasets, rses):
    if comments==True:
        print("\nGet a list of all the files in a dataset.")
    list_of_files=[]
    datasets_leangth={}
    for n in tqdm(range(len(datasets[:5])), disable=tqmdis):
        dataset=datasets[n]
        if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
            pass
        else:
            L=(os.popen("rucio list-file-replicas {}".format(dataset)).read()).split('\n')
            [file1 for file1 in L]
            list_of_files.extend(L)
            datasets_leangth[dataset]=len(L)

    filelocations_rse=[]
    #print(list_of_files)
    for rse in rses:
        if "GRIDFTP" in rse:
            newlist=[file for file in list_of_files if rse in file]
        else:
            newlist=[file for file in list_of_files if rse in file and "GRIDFTP" not in file]
        new_new_list=[file.replace(" ","").split('|') for file in newlist]
        new_new_list=[file[-2].replace(file[2],"") for file in new_new_list]
        new_new_list=[file.replace("{}:file://".format(rse),"") for file in new_new_list]
    #print(new_new_list)

    for addres in new_new_list:
        if addres not in filelocations_rse:
            filelocations_rse.append(addres)
            
    #print(filelocations_rse)
    return([filelocations_rse,datasets_leangth])




#scopes=list_scopes()
scopes=["mc20"]
All_datasets=get_all_datasets(scopes)[0]
datasets_leangth_rse=get_all_datasets(scopes)[1]
rses=["LUND"]
datasets_rse=files_from_datasets(All_datasets,rses)

