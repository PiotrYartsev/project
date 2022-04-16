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



for n in tqdm(range(len(output_list_to_check))):
    output_file=output_list_to_check[n]
    summery_problems=[]  
    files_batch_not_a_problem=[]
    files_really_problem_batch=[]
    files_missing_batch=[]
    files_missing_rucio_list=[]

    files_in_output=next(os.walk('output/{}'.format(output_file)))[2]
    #print(files_in_output)
    if "files_missing_storage.txt" in files_in_output:

        files_missing_storage=open("output/{}/files_missing_storage.txt".format(output_file),"r")

        files_missing_storage_lines=files_missing_storage.readlines()
        files_missing_storage_list=[]


        datasets_and_numbers=open("output/{}/datasets_and_numbers.txt".format(output_file),"r")
        datasets_and_numbers_list=[]

        for line in datasets_and_numbers:
            datasets_and_numbers_list.append(line.split(","))
        datasets_and_numbers.close()


        for file in datasets_and_numbers_list:
            batch=file[0]
            #print(file)
            number=int(file[1])
            n=0
            for stuff in files_missing_storage_lines:
                if batch in stuff:
                    n=n+1
            if n/number>0:
                if n/number>0.2 and not n/number==1:
                    files_really_problem_batch.append([batch,n/number])
                elif n/number==1:
                    files_missing_batch.append([batch,n/number])
                else:
                    files_batch_not_a_problem.append([batch,n/number])
            else:
                files_batch_not_a_problem.append([batch,n/number])


        files_really_problem_batch_list=[]
        for file in files_really_problem_batch:
            file=file[0]
            for stuff in files_missing_storage_lines:
                if file in stuff:
                    output=stuff.replace("\n","").split(",")
                    
                    files_really_problem_batch_list.append(output)

        files_missing_batch_list=[]
        for file in files_missing_batch:
            file=file[0]

            for stuff in files_missing_storage_lines:
                if file in stuff:
                    output=stuff.replace("\n","").split(",")
                    files_missing_batch_list.append(output)

        files_batch_not_a_problem_list=[]
        for file in files_batch_not_a_problem:
            file=file[0]
            for stuff in files_missing_storage_lines:
                if file in stuff:
                    output=stuff.replace("\n","").split(",")
                    files_batch_not_a_problem_list.append(output)

        if not os.path.exists('classifier/{}'.format(output_file)):
            os.makedirs('classifier/{}'.format(output_file))

        if not os.path.exists('classifier/{}/files_missing_storage'.format(output_file)):
            os.makedirs('classifier/{}/files_missing_storage'.format(output_file))

        files_really_problem_file=open('classifier/{}/files_missing_storage/batch_problem.txt'.format(output_file),"w+")
        for file in files_really_problem_batch_list:
            output=file[0]+","+file[1]+","+file[2]+"\n"
            files_really_problem_file.write(output)
        files_really_problem_file.close()

        files_missing_batch_file=open('classifier/{}/files_missing_storage/batch_missing.txt'.format(output_file),"w+")
        for file in files_missing_batch_list:
            output=file[0]+","+file[1]+","+file[2]+"\n"
            files_missing_batch_file.write(output)
        files_missing_batch_file.close()

        files_other_problem_file=open('classifier/{}/files_missing_storage/other_problem.txt'.format(output_file),"w+")
        for file in files_batch_not_a_problem_list:
            output=file[0]+","+file[1]+","+file[2]+"\n"
            files_other_problem_file.write(output)
        files_other_problem_file.close()

        #make the messages
        for stuff in files_missing_batch:
            file=stuff[0]
            procentage=stuff[1]
            output=("The batch {} is present in Rucio but is missing from storage.".format(file))
            summery_problems.append(output)

        for stuff in files_really_problem_batch:
            file=stuff[0]
            procentage=stuff[1]*100
            output=("The batch {} in Rucio migh have some problem, becouse {} % of it is missing in storage.".format(file,procentage))
            summery_problems.append(output)
         
    if "files_missing_rucio.txt" in files_in_output:
            
            #get all the files tat exist in storage but not in Rucio
            files_missing_rucio=open("output/{}/files_missing_rucio.txt".format(output_file),"r")

            files_missing_rucio_lines=files_missing_rucio.readlines()

            #print(len(files_missing_rucio_lines))

            
            files_missing_rucio_files=[] 
            for line in files_missing_rucio_lines:
                line=line.replace("[","")
                line=line.replace("'","")
                line=line.split("]")
                line=[file.split(",") for file in line]
                line=[file for file in line if len(file)==2]
                
                files_missing_rucio_files.extend(line)

            files_missing_rucio_list={} 
            for file in files_missing_rucio_files:

                file_with_no_time=file[0][:file[0].rindex("_")]

                if file[1] in files_missing_rucio_list:
                    files_missing_rucio_list[file[1]].append(file[0])
                else:
                    files_missing_rucio_list[file[1]]=[file[0]]
            

            files_with_copy={}
            for directory in files_missing_rucio_list:

                list_of_files_in_storage=list(os.popen("ls {}".format(directory)))

                for n in range(len(files_missing_rucio_list[directory])):
                    
                    file=files_missing_rucio_list[directory][n]

                    file_with_no_time=file[:file.rindex("_")]


                    files_with_pattern=[]
                    for subfile in list_of_files_in_storage:
                        if (file_with_no_time+"_") in subfile:
                            files_with_pattern.append(subfile)
                    if len(files_with_pattern)>1:
                        files_with_copy[file]=files_with_pattern
            #print(files_with_copy)
            #files_to_sheck=[]
                

    

    
    if len(summery_problems)>0:
        summery_problems_file=open('classifier/{}/summery_problems.txt'.format(output_file),"w+")
        for file in summery_problems:
            output=file+"\n"
            summery_problems_file.write(output)
        summery_problems_file.close()
        #print(summery_problems)

        

"""

for directory in files_missing_rucio_list:
                for n in tqdm(range(len(files_missing_rucio_list[directory]))):
                    file=files_missing_rucio_list[directory][n]
                    #print(directory)
                    #print(file)
                    list_of_files_in_storage=list(os.popen("ls {} | grep {}_*".format(directory,file)))
                    if len(list_of_files_in_storage)>1:
                        print(len(list_of_files_in_storage))
                        print(file)



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