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

def get_runs():
    output_list=next(os.walk('output'))[1]

    checked=[]

    checked_file=open("{}".format('classifier/checked.txt'),"r")
    checked_file_lines=checked_file.readlines()
    for line in checked_file_lines:
        checked.append(line)
    checked_file.close()
    output_list_to_check=[file for file in output_list if file not in checked]
    return(output_list_to_check,checked)

def get_files_in_runs(output_list_file):
    files_in_output=next(os.walk('output/{}'.format(output_list_file)))[2]
    return(files_in_output)


def files_missing_storage_with_datasets(output_file):
    files_batch_not_a_problem=[]
    files_really_problem_batch=[]
    files_missing_batch=[]
    files_missing_rucio_list=[]
    summery_problems=[] 
    
    

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

    
    files_really_problem_batch_dict={}
    for file in files_really_problem_batch:

        file=file[0]
        for stuff in files_missing_storage_lines:
            if file in stuff:
                output1=stuff.replace("\n","").split(",")
                batch=output1[2]
                output=output1[:1]

                if batch in files_really_problem_batch_dict:
                    files_really_problem_batch_dict[batch].append(output)
                else:
                    files_really_problem_batch_dict[batch]=[]
                    files_really_problem_batch_dict[batch].append(output)
   

    files_missing_batch_dict={}
    for file in files_missing_batch:

        file=file[0]
        for stuff in files_missing_storage_lines:
            if file in stuff:
                output1=stuff.replace("\n","").split(",")
                batch=output1[2]
                output=output1[:1]

                if batch in files_missing_batch_dict:
                    files_missing_batch_dict[batch].append(output)
                else:
                    files_missing_batch_dict[batch]=[]
                    files_missing_batch_dict[batch].append(output)

    files_batch_not_a_problem_list1=[]
    for file in files_batch_not_a_problem:
        file=file[0]
        for stuff in files_missing_storage_lines:
            if file in stuff:
                output=stuff.replace("\n","").split(",")
                files_batch_not_a_problem_list1.append(output)
    files_batch_not_a_problem_list=[]
    problem_locations_list_files={}
    if os.path.exists(("output/{}/problem_locations.txt".format(output_file))):
        problem_locations=open("output/{}/problem_locations.txt".format(output_file),"r")
        problem_locations_lines=problem_locations.readlines()
        

        for file in files_batch_not_a_problem_list1:
            directory=file[1]
            if directory+"\n" in problem_locations_lines:
                if directory in problem_locations_list_files:
                    problem_locations_list_files[directory].append(file[0])
                else:
                    problem_locations_list_files[directory]=[]
                    problem_locations_list_files[directory].append(file[0])
            else:
                files_batch_not_a_problem_list.append(file)
    else:
        files_batch_not_a_problem_list=files_batch_not_a_problem_list1


    if not os.path.exists('classifier/{}'.format(output_file)):
        os.makedirs('classifier/{}'.format(output_file))

    if not os.path.exists('classifier/{}/files_missing_storage'.format(output_file)):
        os.makedirs('classifier/{}/files_missing_storage'.format(output_file))

    if len(files_really_problem_batch_dict)>0:
        if not os.path.exists('classifier/{}/files_missing_storage/batch_problem/'.format(output_file)):
            os.makedirs('classifier/{}/files_missing_storage/batch_problem/'.format(output_file))
    if len(files_missing_batch_dict)>0:
        if not os.path.exists('classifier/{}/files_missing_storage/batch_missing//'.format(output_file)):
            os.makedirs('classifier/{}/files_missing_storage/batch_missing//'.format(output_file))
    if len(problem_locations_list_files)>0:
        if not os.path.exists('classifier/{}/files_missing_storage/problem_locations_files//'.format(output_file)):
            os.makedirs('classifier/{}/files_missing_storage/problem_locations_files//'.format(output_file))
    
    if len(files_really_problem_batch_dict)>0:
        for batch in files_really_problem_batch_dict:
            files_really_problem_file=open('classifier/{}/files_missing_storage/batch_problem/{}.txt'.format(output_file,batch),"w+")
            for file in files_really_problem_batch_dict[batch]:
                output=file[0]+"\n"
                files_really_problem_file.write(output)
            files_really_problem_file.close()


    if len(problem_locations_list_files)>0:
        for direct in problem_locations_list_files:
            problem_locations_list_files_file=open('classifier/{}/files_missing_storage/problem_locations_files/problem_locations_files.txt'.format(output_file,direct),"w+")
            output_1=""
            for file in problem_locations_list_files[direct]:
                output_1=output_1+file
            output=direct+","+output_1+"\n"
            problem_locations_list_files_file.write(output)
            problem_locations_list_files_file.close()


    if len(files_missing_batch_dict)>0:
        for batch in files_missing_batch_dict:
            files_missing_batch_file=open('classifier/{}/files_missing_storage/batch_missing/{}.txt'.format(output_file,batch),"w+")
            for file in files_missing_batch_dict[batch]:
                output=file[0]+"\n"
                files_missing_batch_file.write(output)
            files_missing_batch_file.close()

    if len(files_batch_not_a_problem_list)>0:
        files_other_problem_file=open('classifier/{}/files_missing_storage/other_problem.txt'.format(output_file),"w+")
        for file in files_batch_not_a_problem_list:
            output=file[0]+","+file[1]+"\n"
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


def files_missing_rucio(output_file):
    #get all the files tat exist in storage but not in Rucio
    files_missing_rucio=open("output/{}/files_missing_rucio.txt".format(output_file),"r")

    files_missing_rucio_lines=files_missing_rucio.readlines()
    
    files_found_storage=open("output/{}/files_found_storage.txt".format(output_file),"r")
    files_found_storage_list=files_found_storage.readlines()
    files_found_storage_list=files_found_storage_list[0].replace("'","").split(",")
    
    print(files_found_storage_list[:8])
    files_missing_rucio_files=[] 
    for line in files_missing_rucio_lines:
        line=line.replace("[","")
        line=line.replace("'","")
        line=line.split("\n")
        line=[item.split(",") for item in line]
        line=[item for item in line if len(item)>1]
        files_missing_rucio_files.extend(line)
    
    files_missing_rucio_files_dict={}
    #print(len(files_missing_rucio_files))
    for file in files_missing_rucio_files:
        filename=file[0]

        filename_list=filename[:filename.rindex("_")]
        #print(filename_list)
        if filename_list in files_missing_rucio_files_dict:
            files_missing_rucio_files_dict[filename].append(file)
        else:
            files_missing_rucio_files_dict[filename]=[]
            files_missing_rucio_files_dict[filename].append(file)
    #print(len(files_missing_rucio_files_dict))

    files_missing_rucio_files_dict2={}

#    for n in files_missing_rucio_files_dict:


    




def runner(output_list_to_check,files_in_output):
    if "files_missing_storage.txt" in files_in_output:
        if "datasets_and_numbers.txt" in files_in_output:
            files_missing_storage_with_datasets(output_list_to_check)
    if "files_missing_rucio.txt" in files_in_output:
        files_missing_rucio(output_list_to_check)
    


output_list_to_check=get_runs()[0]
for k in range(len(output_list_to_check)):
    output_file=get_files_in_runs(output_list_to_check[k])

    #output_file=get_files_in_runs("2022-04-21_21:52:37.144536")
    #print(output_file)

    runner(output_list_to_check[k],output_file)

 

def pointless():
    for n in tqdm(range(len(output_list_to_check))):
        

        
        #print(files_in_output)
        
        
                
                
                    

        

        
        if len(summery_problems)>0:
            summery_problems_file=open('classifier/{}/summery_problems.txt'.format(output_file),"w+")
            for file in summery_problems:
                output=file+"\n"
                summery_problems_file.write(output)
            summery_problems_file.close()
            #print(summery_problems)

            
