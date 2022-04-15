import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from run_test import *
from numpy import size

if not os.path.exists('classifier'):
        os.makedirs('classifier')
if not os.path.exists('classifier/checked.txt'):
        os.mknod('classifier/checked.txt')

output_list=next(os.walk('output'))[1]

checked=[]

checked_file=open("{}".format('classifier/checked.txt'),"r")
checked_file_lines=checked_file.readlines()
for line in checked_file_lines:
    checked.append(line)
checked_file.close()


output_list_to_check=[file for file in output_list if file not in checked]
#print(output_list_to_check)
    

for output in output_list_to_check:
    files_in_output=next(os.walk('output/{}'.format(output)))[2]
    #print(files_in_output)
    if "files_missing_storage.txt" in files_in_output:
        #print("yes")
        files_missing_storage=open("output/{}/files_missing_storage.txt".format(output),"r")

        files_missing_storage_lines=files_missing_storage.readlines()
        #print(files_missing_storage_lines)
        files_missing_storage_list=[]

        for line in files_missing_storage:
            #print(line)
            files_missing_storage_list.append(line.split(","))
        files_missing_storage.close()

        datasets_and_numbers=open("output/{}/datasets_and_numbers.txt".format(output),"r")
        datasets_and_numbers_list=[]

        for line in datasets_and_numbers:
            #print(line)
            datasets_and_numbers_list.append(line.split(","))
        datasets_and_numbers.close()

        files_problem_batch=[]
        files_really_problem_batch=[]
        files_missing_batch=[]

        for file in datasets_and_numbers_list:
            batch=file[0]
            number=int(file[1])
            n=0
            for stuff in files_missing_storage_lines:
                if batch in stuff:
                    n=n+1
            if n/number>0:
                if n/number>0.1 and n/number<0.2:
                    files_problem_batch.append(batch)
                elif n/number>0.2 and not n/number==1:
                    files_really_problem_batch.append(batch)
                elif n/number==1:
                    files_missing_batch.append(batch)    

        files_problem_batch_list=[]
        for file in files_problem_batch:
            for stuff in files_missing_storage_lines:
                if file in stuff:
                    files_problem_batch_list.append(stuff)

        files_really_problem_batch_list=[]
        for file in files_really_problem_batch:
            for stuff in files_missing_storage_lines:
                if file in stuff:
                    files_really_problem_batch_list.append(stuff)

        files_missing_batch_list=[]
        for file in files_missing_batch:
            for stuff in files_missing_storage_lines:
                if file in stuff:
                    files_missing_batch_list.append(stuff)

        if len(files_problem_batch_list)>0:
            for file_info in files_problem_batch_list:
                file=file_info.split(",")
                files_problem_batch_txt=open("output/{}/files_problem_batch.txt".format(output),"w+")
                files_problem_batch_txt.write()
                


        print(len(files_problem_batch_list))
        print(len(files_really_problem_batch_list))
        print(len(files_missing_batch_list))

        
           



        

"""
while open("missing") as f:
    for line in f:
        listOfLine=line.split(",")


def get_dataset_number(listOfLine):
    newlist=[]
    for n in listOfLine:
        newlist.append(n.split("_"))
    return(newlist)
    
for n in newlist:
    files_to_Check=os.popen("ls -l | grep {}".format(n))
    files_to_Check=files_to_Check.split("_")
    timestamp_data=files_to_Check[-1].replace(".root","")
    timestamp_data=files_to_Check[-1].replace("t","")
    timestamp_missing=newlist[-1].replace(".root","")
    timestamp_missing=newlist[-1].replace("t","")
    if abs(int(timestamp_data)-int(timestamp_missing))>=3600:
        

def problem_with_batch():
    for n in list_of_batches:
        #save the number of files in a dataset in the other script 
        batchname=n[0]
        batchnumber=n[1]

        for k in batchname:
            integer=0
            for n in lines:
                if k==n[0]:
                    integer+=1
        
    if integer/batchnumber>0.2:
        print("problem with batch {}".format(batchname))

for corrupted_file in list_of_corrupted_files:
    if size(rucio_file)/size(corrupted_file)<0.8:
        print("File is very corrupted {}/data is missing".format(corrupted_file))
    else:
        print("File is not very corrupted {}".format(corrupted_file))

def corruption_of_storage():
    #find out what harddrive the storage was located on 

#does the sotrage have a regester of commands run on it?


#make a list of adresses of the storage
#count the number of files there
#compare
#do for all scopes 
for missing_file in list_of_missing_files:
    when_were_they_created
    same_time=simulation_run[]
    if many-files-created at around a same_time:
        print("Probablu a problem with the run {} that was at {}".format(simulation_run, same_time))
"""