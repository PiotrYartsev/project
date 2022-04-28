import os
from zlib import adler32
from datetime import datetime
from tqdm import *
from run_test import *
from numpy import size
import test2



if not os.path.exists('classifier'):
        os.makedirs('classifier')
if not os.path.exists('classifier/checked.txt'):
        os.mknod('classifier/checked.txt')

def get_runs():
    output_list=next(os.walk('output'))[1]

    checked=[]

    checked_file=open("{}".format('classifier/checked.txt'),"r+")
    checked_file_lines=checked_file.readlines()
    for line in checked_file_lines:
        checked.append(line)
    output_list_to_check=[]
    for file in output_list:
        if file not in checked:
            output_list_to_check.append(file)
    #output_list_to_check=[file for file in output_list if file not in checked]
    for file in output_list:
        output=file+"\n"
        checked_file.write(output)
    checked_file.close()
    return(output_list_to_check,checked)

def get_files_in_runs(output_list_file):
    files_in_output=next(os.walk('output/{}'.format(output_list_file)))[2]
    return(files_in_output)


def files_missing_storage_with_datasets(output_file):
    files_batch_not_a_problem=[]
    files_really_problem_batch=[]
    files_missing_batch=[]
    files_missing_rucio_list=[]
    
    
    

    files_missing_storage=open("output/{}/files_missing_storage.txt".format(output_file),"r")
    files_missing_storage_lines=files_missing_storage.readlines()
    files_missing_storage_list=[]



    datasets_and_numbers=open("output/{}/datasets_and_numbers.txt".format(output_file),"r")
    datasets_and_numbers_list=[]
    print("Extracting information about datasets.")
    for line in tqdm(datasets_and_numbers):
        
        datasets_and_numbers_list.append(line.split(","))
    datasets_and_numbers.close()

    print("Counting and comparing the number of files missing in storage and files registered to dataset.")
    for file in tqdm(datasets_and_numbers_list):
        
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

    print("Cleaning the output.")
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
            
    print("Writing output to files.")

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
    
    


    runs={}
    for files in files_found_storage_list:
        file=files.replace("\n","")
        #file=files.split(",")
        
        filename_list=file[:file.rindex("_")]
        
        filename=filename_list[:filename_list.rindex("_")]
        filename=filename.replace(" ","")
        if not filename in runs:
            runs[filename]=1
        else:
            runs[filename]=runs[filename]+1

    files_missing_rucio_files=[] 
    print('Cleaning the missing from Rucio data')
    for n in tqdm(range(len(files_missing_rucio_lines))):
        line=files_missing_rucio_lines[n]
        line=line.replace("[","")
        line=line.replace("'","")
        line=line.split("\n")
        line=[item.split(",") for item in line]
        line=[item for item in line if len(item)>1]
        files_missing_rucio_files.extend(line)
    
    files_missing_rucio_files_dict={}
    
    print('Looking for duplicates among missing files and extraxting file name')
    for n in tqdm(range(len(files_missing_rucio_files))):
        file=files_missing_rucio_files[n]
        filename=file[0]
        address=file[1]

        filename_list=filename[:filename.rindex("_")+1]
        
        if filename_list in files_missing_rucio_files_dict:
            files_missing_rucio_files_dict[filename_list].append(filename)
        else:
            files_missing_rucio_files_dict[filename_list]=[]
            files_missing_rucio_files_dict[filename_list].append(address)
            files_missing_rucio_files_dict[filename_list].append(filename)
    

    files_missing_rucio_files_dict2={}
    many=[]
    few=[]
    print("Search for duplicates among the files in Rucio and in storage")
    for n in tqdm(files_missing_rucio_files_dict):
        
        k=[file for file in files_found_storage_list if n in file]
        
        files_missing_rucio=files_missing_rucio_files_dict[n]

        files_missing_rucio.extend(k)
        if len(k)>0:
            many.append(files_missing_rucio)
        else:
            few.append(files_missing_rucio)
    print("Among the files missing from Rucio {} were duplicates {} are regular missing files".format(len(many),len(few)))

    

    
    adler_problem=[]
    adler_no_problem=[]
    print("Calculate checksum for duplicates to se if they are corrupted or just a copy.")
    for n in tqdm(range(len(many))):
        file=many[n]
        addres=file[0].replace(" ","")
        adler32=[]
        for filename in file[1:]:
            filename.replace(" ","")
            fulladress=addres+filename
            fulladress=fulladress.replace(" ","")
            adler32output=get_adler32_checksum(fulladress)
            adler32.append(adler32output)

        adler32=list(set(adler32))
        if len(adler32)>1:
            adler_problem.append(file[1:])
        else:
            adler_no_problem.append(file[1:])
    print("Number of currupted files")
    print(len(adler_problem))
    
    problem_runs={}
    for file in adler_problem:
        for filename in file:
            filename=filename[:filename.rindex("_")]
            filename_name=filename[:filename.rindex("_")]
            filename_name=filename_name.replace(" ","")
            if filename_name not in problem_runs:
                problem_runs[filename_name]=0
            else:
                problem_runs[filename_name]=problem_runs[filename_name]+1


    problem_runs_2=[]
    missing_runs_2=[]
    no_problem_runs_2=[]
    for filename in problem_runs:
        number_problem_files=problem_runs[filename]
        if number_problem_files==0:
            no_problem_runs_2.append(filename+","+str(0))
        else:
            number_all=runs[filename]

            frac=number_problem_files/number_all

            if frac>0.2 and not frac==1:
                problem_runs_2.append(filename+","+str(frac))
            elif frac==1:
                missing_runs_2.append(filename+","+str(frac))
            else:
                no_problem_runs_2.append(filename+","+str(frac))
    print("runs with problem")
    print(len(problem_runs_2))
    print("runs missing problem")
    print(len(missing_runs_2))
    print("runs no problem")
    print(len(no_problem_runs_2))


    if not os.path.exists('classifier/{}'.format(output_file)):
        os.makedirs('classifier/{}'.format(output_file))

    if not os.path.exists('classifier/{}/files_missing_rucio'.format(output_file)):
        os.makedirs('classifier/{}/files_missing_rucio'.format(output_file))

    if len(adler_problem)>0:
        currupted_files=open('classifier/{}/files_missing_rucio/currupted_duplicate.txt'.format(output_file),"w+")
        for file in adler_problem:
            output=""
            for name in file:
                output=output+","+name
            output=output+"\n"
            currupted_files.write(output)
        currupted_files.close()
    
    if len(adler_no_problem)>0:
        regular_files=open('classifier/{}/files_missing_rucio/regular_duplicate.txt'.format(output_file),"w+")
        for file in adler_no_problem:
            output=""
            for name in file:
                output=output+","+name
            output=output+"\n"
            regular_files.write(output)
        regular_files.close()

    if len(problem_runs_2)>0:
        runs_with_problem_file=open('classifier/{}/files_missing_rucio/runs_with_problem.txt'.format(output_file),"w+")
        for file in problem_runs_2:
            output=file+"\n"
            runs_with_problem_file.write(output)
        runs_with_problem_file.close()
    
    if len(missing_runs_2)>0:
        missing_runs_files=open('classifier/{}/files_missing_rucio/missing_runs_2.txt'.format(output_file),"w+")
        for file in missing_runs_2:
            output=file+"\n"
            missing_runs_files.write(output)
        missing_runs_files.close()

    if len(few)>0:
        missing_files=open('classifier/{}/files_missing_rucio/missing.txt'.format(output_file),"w+")
        #print((few))
        for file_out in few:
            output=str(file_out)+"\n"
            missing_files.write(output)
        missing_files.close()

    for stuff in problem_runs_2:
        stuufer=stuff.split(",")
        file=stuufer[0]
        procentage=str((float(stuufer[1])*100))
        output=("The run {} in Rucio migh have some problem, becouse {} % of the files missing in Rucio are duplicates.".format(file,procentage))
        summery_problems.append(output)

    for stuff in missing_runs_2:
        stuufer=stuff.split(",")
        file=stuufer[0]
        procentage=str((float(stuufer[1])*100))
        output=("The run {} in Rucio probably has some problem, becouse {} % of the files missing in Rucio are duplicates.".format(file,procentage))
        summery_problems.append(output)

def adler32fail(output_file):
    adler32failfile=open("output/{}/adler32_fail.txt".format(output_file),"r")
    adler32failfile_lines=adler32failfile.readlines()
    for line in adler32failfile_lines:
        lines=line.split(",")
        if ['\n']==lines:
            pass
        else:
            filename=lines[0]
            rucio=lines[1]
            storage=lines[2].replace("\n","")
            output=("The file {} has been corrupted. The value for the Adler32 checksum in Rucio is {} but the checksum in storage is {}.".format(filename,rucio,storage))
            summery_problems.append(output)

def runner(output_list_to_check,files_in_output):
    if "files_missing_storage.txt" in files_in_output:
        if "datasets_and_numbers.txt" in files_in_output:
            print("\nClassifying files present in Rucio but missing from storage.\n")
            files_missing_storage_with_datasets(output_list_to_check)
    if "files_missing_rucio.txt" in files_in_output:
        print("\nClassifying files present in storage but missing from Rucio.\n")
        files_missing_rucio(output_list_to_check)
    if "adler32_fail.txt" in files_in_output:
        adler32fail(output_list_to_check)
    if len(summery_problems)>0:
        print("\nA summery of the problems found\n")
        summery_problems_file=open('classifier/{}/summery_problems.txt'.format(output_list_to_check),"w+")
        for p in summery_problems:
            print(p)
            output=str(p)+"\n"
            summery_problems_file.write(output)
        summery_problems_file.close()
    
    
    


output_list_to_check=get_runs()[0]
for k in range(len(output_list_to_check)):
    summery_problems=[] 
    if "All" not in output_list_to_check[k]:
        pass
    else:
        output_file=get_files_in_runs(output_list_to_check[k])

        #output_file=get_files_in_runs("2022-04-21_21:52:37.144536")
        #print(output_file)

        runner(output_list_to_check[k],output_file)
    