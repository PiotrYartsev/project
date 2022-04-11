


from numpy import size


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