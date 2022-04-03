import os
from zlib import adler32
from datetime import datetime
from tqdm import *
tqmdis=False


def list_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   for n in range(len(scopes)):
       scopes[n]=scopes[n].replace("\n","")
   return(scopes)


def get_datasets_from_scope(scope):
    #currently broken, have to figure out why
    datasets_scope=list(os.popen("rucio list-dids --filter type=DATASET {}:*".format(scope)))
    [ x for x in datasets_scope if "---------------------------------" not in x ]
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

def files_from_datasets(datasets, rses):
    print("Get a list of all the files in a dataset")
    list_of_files=[]
    for n in tqdm(range(len(datasets[:2])), disable=tqmdis):
        dataset=datasets[n]
        L=(os.popen("rucio list-file-replicas {}".format(dataset)).read()).split('\n')
        list_of_files.extend(L)

    datasets_rse={}
    for rse in rses:
        print(rse)
        newlist=[file for file in list_of_files if rse in file]
        print(len(newlist))
        new_new_list=[file.replace(" ","").split('|') for file in newlist]
        new_new_list=[file[2:-1] for file in new_new_list]
        datasets_rse[rse]=new_new_list
    return(datasets_rse)
        
def clean_up_datasets_rse(datasets_rse):
    for rse in datasets_rse:
        print("Cleaning up data about files at {}".format(rse))
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
    with open("{}".format(file2),"rb") as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2**32
    return(asum)



#compares the adler34 checksum for the files we matched between storage and rucio. Outputs the rucio entries without a match, the checksum of the file in storage, the checksum as reported by rucio.
def compere_checksum(not_missing_files):
    not_missing_files_file = open(not_missing_files, 'r')
    lines_not_missing_files_file = not_missing_files_file.readlines()
    not_missing_files_file_name=not_missing_files[not_missing_files.rindex('/')+1:]
    not_missing_files_file_address=not_missing_files.replace(not_missing_files_file_name,'')
    new_error_file="adler32_fail" + not_missing_files_file_name
    integ=0
    dataset=not_missing_files_file_address.replace('/home/pioyar/Desktop/project/output/','')
    dataset=dataset.replace('/not_missing/','')
    print("Comparing the adler32 checksum in rucio with checksum in storage for dataset {}.".format(dataset))
    for n in tqdm(range(len(lines_not_missing_files_file)), disable=tqmdis):
        line=lines_not_missing_files_file[n]
        line_list=line.split(",")
        address=line_list[0].replace(' ','')
        
        checksum_dec=get_adler32_checksum(address)
        checksum_hex=hex(checksum_dec)
        checksum_hex=checksum_hex.lstrip("0x").rstrip("L")
        
        checksum_rucio=line_list[2].replace(' ','')
        checksum_rucio=checksum_rucio.replace('\n','')
        if str(checksum_hex)==checksum_rucio:
            pass
        else:
            integ=integ+1
            os.system("echo {},{},{} >> {}/adler32check/{}".format(line_list[1],checksum_rucio,str(checksum_hex),not_missing_files_file_address,new_error_file))
            #print(new_error_file)
    print("We found {} corrupted files out of {}".format(integ,len(lines_not_missing_files_file)))


scopes=(list_scopes())


All_datasets=get_all_datasets(scopes)


rses=list_rse()


datasets_rse=files_from_datasets(All_datasets,rses)


datasets_rse=clean_up_datasets_rse(datasets_rse)