import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from run_test import *


#initialize defoult values 
tqmdis=False
comments=False
limit=0
checksum=True


#gets all the valid scopes registered in Rucio
def list_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   for n in range(len(scopes)):
       scopes[n]=scopes[n].replace("\n","")
   return(scopes)


#For a scope get all datasets registered in rucio for that scope 1.
def get_all_datasets(scopes):
    All_datasets=[]
    #Iterate over all scopes, run function get_datasets_from_scope fro scope
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



def files_from_datasets(datasets, rses):
    datasets=list(set(datasets))
    if comments==True:
        print("\nGet a list of all the files in a dataset.")
    
    datasets_rse={}
    for rse in rses:
        list_of_files=[]
        number_of_files_in_dataset={}
        if limit==0:
            for n in tqdm(range(len(datasets)), disable=tqmdis):
                dataset=datasets[n]
                if "SCOPE:NAME[DIDTYPE]" in dataset or "-------------------------" in dataset:
                    pass
                else:
                    L=(os.popen("rucio list-file-replicas --rses {} {}".format(rse,dataset)).read()).split('\n')
                    L=[file for file in L if rse in file]
                    number_of_files_in_dataset[dataset]=len(L)
                    list_of_files.extend(L)
        else:
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
        number_of_files_in_dataset = dict( [(k,v) for k,v in number_of_files_in_dataset.items() if not v==0])
        print(len(list_of_files))
        list_of_files=list(set(list_of_files))
        print(len(list_of_files))
        new_new_list=[file.replace(" ","").split("|") for file in list_of_files]
        new_new_list=[file[2:] for file in new_new_list]
        datasets_rse[rse]=new_new_list
        
        if comments==True:
            print("\n")
            print("For the rse {} we have this many files:".format(rse))
            print(len(datasets_rse[rse]))

    datasets_rse = dict( [(k,v) for k,v in datasets_rse.items() if len(v)>0])
    return(datasets_rse, number_of_files_in_dataset)
        
def clean_up_datasets_rse(datasets_rse):
    for rse in datasets_rse:
        if comments==True:
            print("\nCleaning up data about files at {}".format(rse))

        dataset_list=datasets_rse[rse]
        #print(dataset_list[10:])
        for n in tqdm(range(len(dataset_list)), disable=tqmdis):
            dataset=dataset_list[n]
            dataset[3]=dataset[3].replace(dataset[0],"")
            dataset[3]=dataset[3].replace("{}:file://".format(rse),"")
    return (datasets_rse)
    

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
def compere_checksum(datasets_rse, number_of_files_in_dataset):
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
    failed_adles32=save_dir+"/"+"adler32_fail.txt"
    not_found_addres=save_dir+"/"+"files_missing_storage.txt"
    found_addres=save_dir+"/"+"files_found_storage.txt"
    missig_in_rucio_addres=save_dir+"/"+"files_missing_rucio.txt"
    datasets_addres=save_dir+"/"+"datasets_and_numbers.txt"
    for rse in datasets_rse:
        directory_list=[]
        files_found={}
        files_not_found={}

        integ=0
        if comments==True:
            print("\nComparing the adler32 checksum in rucio with checksum in storage for rse {}.".format(rse))
        datasets=datasets_rse[rse]
        for n in tqdm(range(len(datasets)), disable=tqmdis):
            file_data=datasets[n]
            print(file_data)
            print(file_data[-1])
            print(file_data[4])

            file=file_data[0]
            
            dataset_file=file_data[-1]

            directory=file_data[3]
            if directory not in directory_list:
                directory_list.append(directory)
            fullpath=directory+file
            
            checksum_rucio=file_data[2]

            checksum_dec=get_adler32_checksum(fullpath)
            
            not_found_str=str(file)+","+directory+","+dataset_file+"\n"
            file=file+"\n"
            if checksum_dec==0:
                if rse in files_found:
                    files_found[rse].append(file)
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(file)
            elif checksum_dec==None:
                if rse in files_not_found:
                    files_not_found[rse].append(not_found_str) 
                else:   
                    files_not_found[rse]=[]
                    files_not_found[rse].append(not_found_str) 
            else:
                if rse in files_found:
                    files_found[rse].append(file) 
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(file)
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

        not_found= open("{}".format(not_found_addres),"w+")

       

        found= open("{}".format(found_addres),"w+")
         
        
        for n in files_not_found:
            for k in files_not_found[n]:
                not_found.write(k)

        for n in files_found:
            found.write(str(files_found[n]))
        
        print("\nFor each storage location find what files in storage is not registered in Rucio.")
        directory_of_files_not_in_rucio=[]
        files_not_in_rucio=[]
        for n in tqdm(range(len(directory_list)), disable=tqmdis):
            directory=directory_list[n]
            stuff_to_add=list(os.popen("ls {}".format(directory)))
            files_not_in_rucio.extend(stuff_to_add)
            
            stuff_to_add2=[]
            for n in range(len(stuff_to_add)):
                stuff_to_add2.append(directory)

            directory_of_files_not_in_rucio.extend(stuff_to_add2)
            #[file.replace("\n","") for file in files_not_in_rucio]
        not_found.close()
        found.close()

        not_in_rucio=open("{}".format(missig_in_rucio_addres),"w+")
        #missig_in_rucio=set(files_not_in_rucio)-set(files_found[rse])
        
        b=set(files_found[rse])

        missig_in_rucio=[i for i, item in enumerate(files_not_in_rucio) if item not in b]
        missig_in_rucio=list(missig_in_rucio)

        missig_in_rucio2=[]
        for n in missig_in_rucio:
            string=str(files_not_in_rucio[n]).replace("\n","")+","+str(directory_of_files_not_in_rucio[n])+"\n"
            missig_in_rucio2.append(string)
        
        print(len(files_found[rse]))
        print(len(files_not_in_rucio))
        print(abs(len(files_not_in_rucio)-len(files_found[rse])))

        print("\nWe found {} missing files in storage that are registered in rucio. PS: Unless you searched all scopes this does not mean much.".format(len(missig_in_rucio)))


        
        for file in missig_in_rucio2:
            not_in_rucio.write(file)
        not_in_rucio.close()
        

    datasets= open("{}".format(datasets_addres),"w+")       
    for dataset in number_of_files_in_dataset:
        output=str(dataset) +","+str(number_of_files_in_dataset[dataset])+"\n"
        datasets.write(output)

    datasets.close()