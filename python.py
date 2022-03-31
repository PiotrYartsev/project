#from rucio.client import Client
#import rucio.client as rucio
#Client= Client()

#l=(Client.list_scopes())
#print(l)
#print(list(Client.list_datasets_per_rse(rse='LUND', limit=10)))
#print(list(Client.get_rse_usage('LUND')))
#print(list(Client.list_rse_attributes('LUND')))
#print(list(Client.list_rses()))
#for scope in l:
#    print(scope)
#    L=(Client.list_replicas([{'scope':'{}'.format(scope)}], rse_expression='LUND'))
#    print(L)

#Why not use python rse wrapper? its inconsistent so I have no idea what to write where becouse sometimes dids have to be scope=mc20 and sometimes {"scope":"mc20"}
import os
from os.path import exists
from zlib import adler32
from datetime import datetime
import subprocess
from subprocess import check_output
from tqdm import *
from os import path
import os.path


os.system("cd; cd rucio-client-venv; source bin/activate")







"""
def get_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   return(scopes)
def get_datasets_scopes(scopes):
    #currently broken, have to figure out why
    datasets_scopes=list(os.popen("rucio list-dids --filter type=DATASET test:*"))
    
    return(datasets_scopes)
def get_datasets_rse(rse):
    #currently broken
    datasets_rse=list(os.popen("rucio list-datasets-rse {}".format(rse))
    return(datasets_rse)
"""



def files_from_datasets(datasets):
    number_of_files=0
    L2=[]
    print("Get a list of all the files in a dataset")
    for n in tqdm(range(len(datasets))):
        dataset=datasets[n]
        L=((os.popen("rucio list-file-replicas {} | grep LUND".format(dataset)).read()).split('\n'))
        #print(L)
        for l in L:
            l2=l.split('|')
            L2.append(l2)
            number_of_files=number_of_files+0
        break
    return(L2)





def count_the_files(directory):
    pass





def get_adler32_checksum(dir2, file2):
    BLOCKSIZE=256*1024*1024
    asum=1
    with open("{}{}".format(dir2,file2),"rb") as f:
        while True:
            data = f.read(BLOCKSIZE)
            if not data:
                break
            asum = adler32(data, asum)
            if asum < 0:
                asum += 2**32
    return(asum)
            



def get_info_from_all_data_storage(rse, directory):
    f = open("/home/pioyar/Desktop/project/files.txt", "w")
    print("Get information about all files at a directory such as their name and their adler32 checksum")
    if rse=="LUND":
        for n in tqdm(range(len(os.listdir(directory)))):
            file=os.listdir(directory)[n]
            adler32_checksum=get_adler32_checksum(directory,file)
            adler32_checksum=hex(adler32_checksum)
            adler32_checksum=adler32_checksum.lstrip("0x").rstrip("L")
            #print(adler32_checksum)
            info_from_data=(file+", "+str(adler32_checksum)+"\n")
            f.write(info_from_data)
            #Temporary limit for test speed
            
            if n>2:
                break
            
    else:
        pass




def get_info_from_some_data_storage(file, directory):
    f = open("/home/pioyar/Desktop/project/files.txt", "w")
    print("Get information about file at a directory such as its name and their adler32 checksum")
    adler32_checksum=get_adler32_checksum(directory,file)
    adler32_checksum=hex(adler32_checksum)
    adler32_checksum=adler32_checksum.lstrip("0x").rstrip("L")
    #print(adler32_checksum)
    #info_from_data=(file+", "+str(adler32_checksum)+"\n")
    return(adler32_checksum)






def check_if_the_file_exist_bash(files_to_search_for_as_list):
    now = datetime.now()
    not_missing="/home/pioyar/Desktop/project/not_missing_{}.txt".format(now).replace(" ","_")
    missing="/home/pioyar/Desktop/project/missing_{}.txt".format(now).replace(" ","_")
    print("For each file in datasets look for it in storage and put the files it matches in  not_missing_timestamp.txt and the dataset entry without a match in missing_timestamp.txt")
    for value in tqdm(range(len(files_to_search_for_as_list)-1)):
        address=(files_to_search_for_as_list[value][5])
        schecksum=(files_to_search_for_as_list[value][4])
        #print(schecksum)
        address=address.replace("LUND: file://", "")
        #print(address)
        fille=address[address.rindex('/')+1:]
        address=address.replace(fille,"")
        fulladdress=address+fille
        #print(address)
        #print(fille)
        #print(exists(address))
        
        os.system("cd; cd {}; test -e {} && echo {} , {} , {} >> {} || echo {} , {} , {} >> {} ".format(address, fulladdress, fulladdress, fille, schecksum, not_missing, fulladdress, fille, schecksum, missing))

    return([not_missing, missing])

def check_if_the_file_exist_python(files_to_search_for_as_list):
    now = datetime.now()
    not_missing="/home/pioyar/Desktop/project/not_missing_{}.txt".format(now).replace(" ","_")
    missing="/home/pioyar/Desktop/project/missing_{}.txt".format(now).replace(" ","_")
    print("For each file in datasets look for it in storage and put the files it matches in  not_missing_timestamp.txt and the dataset entry without a match in missing_timestamp.txt")
    for value in tqdm(range(len(files_to_search_for_as_list)-1)):
        address=(files_to_search_for_as_list[value][5])
        schecksum=(files_to_search_for_as_list[value][4])
        #print(schecksum)
        address=address.replace("LUND: file://", "")
        #print(address)
        fille=address[address.rindex('/')+1:]
        address=address.replace(fille,"")
        address=address.replace(" ","")
        #print(address)
        #print(fille)
        #print(exists(address))
        
        if path.exists("{}{}".format(address,fille)):
            print("yes")
        else:
            print(path.exists("{}{}".format(address,fille)))
            print("{}{}".format(address,fille))
            break
    #return([not_missing, missing])




def compere_checksum(not_missing_files):
    not_missing_files_file = open(not_missing_files, 'r')
    info_from_data=open("/home/pioyar/Desktop/project/files.txt", 'r')
    lines_not_missing_files_file = not_missing_files_file.readlines()
    lines_info_from_data = info_from_data.readlines()
    print(lines_info_from_data[1])
    for line in lines_not_missing_files_file:
        line_list=line.split(",")
        if line_list[0] in lines_info_from_data:
            print(line_list[0])

