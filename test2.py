import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from run_test import *


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
                    
                    address=line.split(",")[1]
                    address.replace('\n',"")
            if comments==True:
                print("\nCleaning up data about files at {}".format(rse))

            dataset_list=datasets_rse[rse]
            #print(dataset_list[:2])
            for n in tqdm(range(len(dataset_list)), disable=tqmdis):
                dataset=dataset_list[n]
                dataset[3]=dataset[3].replace(dataset[0],"")
                
                dataset[3]= dataset[3][dataset[3].index("//")+2:]
                
                dataset[3]= dataset[3][dataset[3].index("/")+1:]
                #print(dataset[3])
                
                dataset[3]=dataset[3].replace("ldcs/",str(address))
                dataset[3]=dataset[3].replace("\n","")
                #print(dataset[3])
                
                
                
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
    

#For a provided file file2 and directory dir2 it will return the adler32 checksum in hexadecimal
def get_adler32_checksum(file2):
    BLOCKSIZE=256*1024*1024
    asum=1
    #If we don't want to compare the checksum and only see if the file exists we try to open the file. If we are able to open the file we set asum=0 and if we fail we set it to None
    if checksum==False:
        try:
            with open("{}".format(file2),"rb") as f:
                asum=0
            f.close()
        except:
            asum=None
    #if we want to do a checksum, we try to open the file. If we fail we set it to None, else we calculate the adler32 checksum for the file.
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
    #Get the current time. Used to name the folders
    now = datetime.now()
    now=str(now)
    now=now.replace(" ","_")
    for rse in datasets_rse:
        now=rse+"_"+now
        break
    if All==True:
        now="All"+"_"+now

    #Check if the Output folder exist, if not create it
    if not os.path.exists('output'):
            print("Output folders missing, generating them for the dataset {}.".format(dataset))
            os.makedirs('output')
    if not os.path.exists('output/{}'.format(now)):
            os.makedirs('output/{}'.format(now))

    #The location for saving the output files
    save_dir='output/{}'.format(now)
    #Address to save the output for the files that failed the adler32 comparison
    failed_adles32=save_dir+"/"+"adler32_fail.txt"
    #Address to save the output for the files that exist in Rucio but are not found in storage
    not_found_addres=save_dir+"/"+"files_missing_storage.txt"
    #Address to save the output for the files that exist in Rucio and are also in storage
    found_addres=save_dir+"/"+"files_found_storage.txt"
    #Address to save the output for the files that exist in storage but not in Rucio
    missig_in_rucio_addres=save_dir+"/"+"files_missing_rucio.txt"
    #Address to save the output for the datasets and the number of files at a rse that exist in that dataset
    datasets_addres=save_dir+"/"+"datasets_and_numbers.txt"


    problem_dir_addres=save_dir+"/"+"problem_locations.txt"
    #A list of directories that are problematic
    problem_dir=[]
    
    for rse in datasets_rse:
        #list of all the directories that we found. Used to get all the files in storage.
        directory_list=[]

        #Dictionary of all the files that we found for a rse (why is this a dict not a list?)
        files_found={}
        #Dictionary of all the files that we did not find for a rse (why is this a dict not a list?)
        files_not_found={}

        #Integer that is increased every time a file failed a checksum
        integ=0
        if comments==True:
            print("\nComparing the adler32 checksum in rucio with checksum in storage for rse {}.".format(rse))
        
        #get all the file at the rse
        datasets=datasets_rse[rse]

        #for each file in
        for n in tqdm(range(len(datasets)), disable=tqmdis):
            #get the information for each file
            file_data=datasets[n]
            #filename
            file=file_data[0]
            
            #the dataset the file belongs to
            dataset_file=file_data[-1]

            #The storage location for where the file is
            directory=file_data[3]

            #If the directory has not yet been encountered, add it to the list directory_list
            if directory not in directory_list:
                directory_list.append(directory)

            #Full path to the file
            fullpath=directory+file
            
            #The checksum that is registered in Rucio
            checksum_rucio=file_data[2]

            #Get the adler32 checksum for the file (or only check if it exist if checksum=False)
            checksum_dec=get_adler32_checksum(fullpath)
            
            #The output to be put in the file with files we did not find in storage
            not_found_str=str(file)+","+directory+","+dataset_file+"\n"

            #added endline so it works in the txt document
            #file=file+"\n"
            #If we had no checksum, if we found the file add it to the file_found dictionary
            if checksum_dec==0:
                if rse in files_found:
                    files_found[rse].append(file)
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(file)
            elif checksum_dec==None:
                #If we did not find the file, add it to the dictionary files_not_found
                if rse in files_not_found:
                    files_not_found[rse].append(not_found_str)
                else:   
                    files_not_found[rse]=[]
                    files_not_found[rse].append(not_found_str)
            else:
                #If we did a checksum and found the file add it to the file_found dictionary
                if rse in files_found:
                    files_found[rse].append(file)
                else:   
                    files_found[rse]=[]
                    files_found[rse].append(file)
                #convert the decimal output from the adler32 checksum to a hexadecimal, so we can compare to Rucio
                checksum_hex=hex(checksum_dec)
                #clean the checksum
                checksum_hex=checksum_hex.lstrip("0x").rstrip("L")
                
                #If the checksums match do nothing, else increase the integer by one and write to the adler32 txt file
                if str(checksum_hex) in checksum_rucio:
                    pass
                else:
                    integ=integ+1
                    os.system("echo {},{},{} >> {}".format(file,checksum_rucio,str(checksum_hex),failed_adles32))
        #get the number of files that failed and succeeded the search in storage
        number_failed_files=0
        number_sucesfull_files=0
        for rse in files_not_found:
            number_failed_files=number_failed_files+len(files_not_found[rse])
        
        for rse in files_found:
            number_sucesfull_files=number_sucesfull_files+len(files_found[rse])

        #print out some information about the comparison
        if comments==True:    
            print("\nFor the rse {} we found that {} files were missing in storage out of {}.".format(rse, number_failed_files, len(datasets)))
            if checksum==True:
                print("We found {} corrupted files out of {}".format(integ,(len(datasets))))

        #Write information about the files we did not find in storage to a txt file
        not_found= open("{}".format(not_found_addres),"w+")
        for n in files_not_found:
            for k in files_not_found[n]:
                not_found.write(k)
        not_found.close()

        #Write information about the files we found in storage to a txt file
        found= open("{}".format(found_addres),"w+")
        for n in files_found:
            for k in files_found[n]:
                output=k+"\n"
                found.write(output)
        found.close()
        


        #A list for the files that exist in storage
        files_in_storage=[]

        #A list of the files that exist in storage but not in Rucio
        files_not_in_Rucio=[]

        

        #For each directory get all the files there for comparison with the files we found
        for n in range(len(directory_list)):
            directory=directory_list[n]
            
            stuff_to_add=list(os.popen("ls {} || echo false".format(directory)))
            if ['false\n']==stuff_to_add:
                #If directory is not found/not available add to problem list
                problem_dir.append(directory)
            else:
                #clean the output
                stuff_to_add=[f.replace("\n","") for f in stuff_to_add]
                #add the directory so it's easier for comparison program and tur each element into a list
                stuff_to_add=[f+","+directory for f in stuff_to_add]
                stuff_to_add=[f.split(",") for f in stuff_to_add]

                files_in_storage.extend(stuff_to_add)
            
                


        if comments==True:
            print("\nFor each storage location find what files in storage is not registered in Rucio.")    
        
        #check if the file in storage exists in the list of found files. If not, add that file to the list files_not_in_Rucio
        for n in tqdm(range(len(files_in_storage)), disable=tqmdis):
            file=files_in_storage[n]
            if file[0] not in files_found[rse]:
                files_not_in_Rucio.append(file)
        
        #Write the files ot in Rucio to a txt file
        not_in_rucio=open("{}".format(missig_in_rucio_addres),"w+")
        for file in files_not_in_Rucio:
            output=str(file[0])+","+str(file[1])+"\n"
            not_in_rucio.write(str(output))
        not_in_rucio.close()
        
        if comments==True:
            print("\nWe found {} files in storage that are not registered in Rucio. PS: Unless you runned a full search for all scopes and datasets this output does not mean much.".format(len(files_not_in_Rucio)))

    #Write the datasets and the number of files in them to a txt file
    datasets= open("{}".format(datasets_addres),"w+")       
    for dataset in number_of_files_in_dataset:
        output=str(dataset) +","+str(number_of_files_in_dataset[dataset])+"\n"
        datasets.write(output)
    datasets.close()

    #If problem files exist, write them to a txt file
    if len(problem_dir)>0:
        prob_loc= open("{}".format(problem_dir_addres),"w+")       
        for direct in problem_dir:
            output=str(direct)+"\n"
            prob_loc.write(output)
        prob_loc.close()