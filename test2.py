import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from run_test import *

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


def list_rse():
    rses=list(os.popen("rucio list-rses"))
    for n in range(len(rses)):
        rses[n]=rses[n].replace("\n","")
    return(rses)



def files_from_datasets(datasets, rses):
    if comments==True:
        print("\nGet a list of all the files in a dataset.")
    list_of_files=[]
    if limit==0:
        for n in tqdm(range(len(datasets)), disable=tqmdis):
            dataset=datasets[n]
            if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
                pass
            else:
                L=(os.popen("rucio list-file-replicas {}".format(dataset)).read()).split('\n')
                [file1+dataset for file1 in L]
                list_of_files.extend(L)
    else:
        if comments==True:
            print("Limit set to {}\n".format(limit))
        for n in tqdm(range(len(datasets[:limit])), disable=tqmdis):
            dataset=datasets[n]
            if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
                pass
            else:
                L=(os.popen("rucio list-file-replicas {}".format(dataset)).read()).split('\n')
                [file1+dataset for file1 in L]
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
    if checksum==False:
        try:
            with open("{}".format(file2),"rb") as f:
                asum=0
            f.close()
        except:
            asum=None
    else:
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
    now = datetime.now()
    now=str(now)
    now=now.replace(" ","_")
    if not os.path.exists('output'):
            #print("Output folders missing, generating them for the dataset {}.".format(dataset))
            os.makedirs('output')
    if not os.path.exists('output/{}'.format(now)):
            #print("Output folders missing, generating them for the dataset {}.".format(dataset))
            os.makedirs('output/{}'.format(now))
    save_dir='output/{}'.format(now)
    failed_adles32="/"+save_dir+"/"+"adler32_fail.txt"
    not_found_addres=save_dir+"/"+"files_not_found.txt"
    found_addres="/"+save_dir+"/"+"files_found.txt"

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

            checksum_dec=get_adler32_checksum(fullpath)
            
            found_not_found_str=str([file,directory])
            found_not_found_str=found_not_found_str+"\n"
            #print(rse)
            if checksum_dec==0:
                if rse in files_found:
                    files_found[rse].append(found_not_found_str) 
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(found_not_found_str)
            elif checksum_dec==None:
                if rse in files_not_found:
                    files_not_found[rse].append(found_not_found_str) 
                else:   
                    files_not_found[rse]=[]
                    files_not_found[rse].append(found_not_found_str) 
            else:
                if rse in files_found:
                    files_found[rse].append(found_not_found_str) 
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(found_not_found_str)
                checksum_hex=hex(checksum_dec)
                checksum_hex=checksum_hex.lstrip("0x").rstrip("L")
                
                
                if str(checksum_hex) in checksum_rucio:
                    pass
                else:
                    integ=integ+1
                    os.system("echo {},{},{} >> {}".format(file,checksum_rucio,str(checksum_hex),failed_adles32))
                    #print(new_error_file)
        
        number_failed_files=0
        number_sucesfull_files=0
        for rse in files_not_found:
            number_failed_files=number_failed_files+len(files_not_found[rse])
        
        for rse in files_found:
            number_sucesfull_files=number_sucesfull_files+len(files_found[rse])

        if comments==True:    
            #print(number_failed_files)
            #print(number_sucesfull_files)

            print("\nFor the rse {} we found that {} files were missing out of {}.".format(rse, number_failed_files, len(datasets)))
            if checksum==True:
                print("We found {} corrupted files out of {}".format(integ,(len(datasets))))
        for n in files_not_found:
            os.system("echo {} >> {}".format(files_not_found[n],not_found_addres))
        for n in files_found:
            os.system("echo {} >> {}".format(files_found[n],found_addres))
