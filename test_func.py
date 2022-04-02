#Why not use python rse wrapper? it's inconsistent so I have no idea what to write where because sometimes dids have to be scope=mc20 and sometimes {"scope":"mc20"}
import os
from zlib import adler32
from datetime import datetime
from tqdm import *

#disable status bar
tqmdis=False

#the part below doesn't work yet, problem with the rucio CLI commands. I will have study further

def get_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   return(scopes)
def get_datasets_scopes(scopes):
    #currently broken, have to figure out why
    datasets_scopes=list(os.popen("rucio list-dids --filter type=DATASET {}:*".format(scopes)))
    
    return(datasets_scopes)
def get_datasets_rse(rse):
    #currently broken
    datasets_rse=list(os.popen("rucio list-datasets-rse {}".format(rse)))
    return(datasets_rse)



#provided a dataset it will look at replicas in that dataset and output all of them in a list. It will only keep the only in the provided rse. To the list, it appends what dataset this entry belongs to.
def files_from_datasets(datasets, rse):
    number_of_files=0
    L2=[]

    print("Get a list of all the files in a dataset")
    for n in tqdm(range(len(datasets)), disable=tqmdis):
        dataset=datasets[n]
        if not os.path.exists('/home/pioyar/Desktop/project/output/{}'.format(dataset)):
            print("Output folders missing, generating them for the dataset {}.".format(dataset))
            os.makedirs('/home/pioyar/Desktop/project/output/{}'.format(dataset))
            os.makedirs('/home/pioyar/Desktop/project/output/{}/missing'.format(dataset))
            os.makedirs('/home/pioyar/Desktop/project/output/{}/not_missing'.format(dataset))
            os.makedirs('/home/pioyar/Desktop/project/output/{}/not_missing/adler32check'.format(dataset))
        L=((os.popen("rucio list-file-replicas {} | grep {}".format(dataset, rse)).read()).split('\n'))
        #print(L)
        L1=[]
        for l in L:
            l2=l.split('|')
            L1.append(l2)
            number_of_files=number_of_files+0
        L2.append([dataset,L1])
    return(L2)




#For every file in storage at the location it creates an entry in files.txt with the file name and its checksum. Probably will not use it later, just a temporary solution. Will only check checksum for files in requested datasets that it already know exist.
def get_info_from_all_data_storage(rse, directory):
    f = open("/home/pioyar/Desktop/project/files.txt", "w")
    print("Get information about all files at a directory such as their name and their adler32 checksum")
    if rse=="LUND":
        for n in tqdm(range(len(os.listdir(directory))), disable=tqmdis):
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




#same as above but uses bash instead of python (much faster). does not calculate adler32 checksum
def get_info_from_all_data_storage2(rse, directory):
    print("Get information about all files at a directory such as their name and their adler32 checksum")
    if rse=="LUND":
        os.system("cd {}; pwd; ls >> /home/pioyar/Desktop/project/files.txt".format(directory))
            
    else:
        pass




#same as the one above the one above, but not limited by rse and has nocomputational limiter
def get_info_from_some_data_storage(file, directory):
    f = open("/home/pioyar/Desktop/project/files.txt", "w")
    print("Get information about file at a directory such as its name and their adler32 checksum")
    adler32_checksum=get_adler32_checksum(directory,file)
    adler32_checksum=hex(adler32_checksum)
    adler32_checksum=adler32_checksum.lstrip("0x").rstrip("L")
    #print(adler32_checksum)
    #info_from_data=(file+", "+str(adler32_checksum)+"\n")
    return(adler32_checksum)




#bash version of the function that finds matches for files in datasets. For each dataset provided it will check if each entry has a match ins storage. If it does it will move the absolute path, name of file and its checksum (provided by rucio) to /home/pioyar/Desktop/project/output/"dataset"/not_missing/not_missing_"timestamp".txt and it does not find a match it moves the same stuff to /home/pioyar/Desktop/project/output/"dataset"/missing/missing_"timestamp".txt
def check_if_the_file_exist_bash(files_to_search_for_as_list_full):
    print("For each file in datasets look for it in storage and put them in differnet txt files depending if they passed or failed.")
    
    for n in range(len(files_to_search_for_as_list_full)):
        now = datetime.now()
        dataset=files_to_search_for_as_list_full[n][0]
        files_to_search_for_as_list=files_to_search_for_as_list_full[n][1]
        not_missing="/home/pioyar/Desktop/project/output/{}/not_missing/not_missing_{}.txt".format(dataset,now).replace(" ","")
        missing="/home/pioyar/Desktop/project/output/{}/missing/missing_{}.txt".format(dataset,now).replace(" ","")

        print("Comparing {} with storage.".format(dataset))
        for value in tqdm(range(len(files_to_search_for_as_list)-1), disable=tqmdis):
            #print(files_to_search_for_as_list)
            address=(files_to_search_for_as_list[value][5])
            schecksum=(files_to_search_for_as_list[value][4])
            #print(dataset)
            address=address.replace("LUND: file://", "")
            fille=address[address.rindex('/')+1:]
            address=address.replace(fille,"")
            fulladdress=address+fille
            #print(address)
            #print(fille)
            #print(exists(address))
            
            os.system("cd; cd {}; test -e {} && echo {},{},{} >> {} || echo {},{},{} >> {} ".format(address, fulladdress, fulladdress, fille, schecksum, not_missing, fulladdress, fille, schecksum, missing))

    #return([not_missing, missing])
    



#does not work, but it is how it is. Perhaps a library will be found that actually works for large folders.
"""
def check_if_the_file_exist_python(files_to_search_for_as_list_full):
    print("For each file in datasets look for it in storage and put the files it matches in  not_missing_timestamp.txt and the dataset entry without a match in missing_timestamp.txt")
    now = datetime.now()
    for n in range(len(files_to_search_for_as_list_full)):
        
        dataset=files_to_search_for_as_list_full[n][0]
        files_to_search_for_as_list=files_to_search_for_as_list_full[n][1]
        not_missing="/home/pioyar/Desktop/project/output/{}/not_missing/not_missing_{}.txt".format(dataset,now).replace(" ","")
        missing="/home/pioyar/Desktop/project/output/{}/missing/missing_{}.txt".format(dataset,now).replace(" ","")

        print("Comparing {} with storage.".format(dataset))
        for value in tqdm(range(len(files_to_search_for_as_list)-1), disable=tqmdis):
            #print(files_to_search_for_as_list)
            address=(files_to_search_for_as_list[value][5])
            schecksum=(files_to_search_for_as_list[value][4])
            #print(dataset)
            address=address.replace("LUND: file://", "")
            fille=address[address.rindex('/')+1:]
            address=address.replace(fille,"")
            fulladdress=address+fille
            #print(address)
            #print(fille)
            #print(exists(address))
            if path.exists(fulladdress):
                print("yes")
            else:
                print("no")
                
                print(path.exists("{}{}".format(address,fille)))
                print("{}{}".format(address,fille))
    #return([not_missing, missing])
"""



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
