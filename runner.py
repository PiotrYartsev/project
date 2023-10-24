#this is the main software that runs all the other parts
from rucio_functions import RucioFunctions

from database_from_rucio import RucioDatasetlocal,RucioDatasetRucio, CustomDataStructure
from argument_loader import get_args, get_datasets_from_args
from multithreading import run_threads
import os
import sqlite3 as sl

#This function returns two list, one of datasets that already exist in the local database and one one for datasets that do not exist in the local database
def use_local_database(datasets,args):
    #check if the local database exist
    if os.path.isfile('local_rucio_database.db'):
        print("Local database found, checking if the datasets exist in the local database\n")
        #connect to the local database
        LocalRucioDataset=sl.connect('local_rucio_database.db')
        #From the table called datasets, which is a kind of table of content, we retrive the scope, name and length of the datasets
        local_database_dataset_data = LocalRucioDataset.execute("""
        SELECT scope, name, length
        FROM dataset
        WHERE exist_at_rses LIKE ?
        """, ('%' + args.rse + '%',)).fetchall()
        #We then check if the datasets exist in the local database. This is done in a multithreaded way. It 
        output=run_threads(thread_count=args.threads,function=RucioDatasetlocal.check_if_exist,data=datasets,const_data=local_database_dataset_data)
        #create a list of datasets that we found in the local database and a list of datasets that we did not find in the local database
        dataset_not_in_local_database=[]
        dataset_already_in_local_database=[]
        for item in output:
            #if its true, put it in databases found in local. If false, put it in databases not found in local
            if item[1]:
                dataset_already_in_local_database.append(item[0])
            else:
                dataset_not_in_local_database.append(item[0])
    else:
        #if the local database does not exist, we dont use it. This exist as I am not sure if we even want to use the local database at all, so this is a way to disable it by not providing it
        print("Local database not found, all datasets will be loaded from Rucio\n")
        dataset_not_in_local_database=datasets
        dataset_already_in_local_database=[]
    return dataset_not_in_local_database,dataset_already_in_local_database


def find_dark_data(Custome_Datasbase, rse, files_in_storage_dump):
    #We find all the files in the custome data structure that have the same rse as the one we are looking for
    files_with_rse=Custome_Datasbase.find_by_metadata("rse",rse)
    if files_with_rse == None:
        print("No files with rse: "+rse+" found for datasets"+str(Custome_Datasbase.dataset_index.keys())+"\n")
        exit(1)
    #We create a new custome data structure with only the files that have the same rse as the one we are looking for
    #FIXME This is hella cluncky, but it works for now
    Files_in_Rucio_data_with_rse=CustomDataStructure()
    Files_in_Rucio_data_with_rse.multiadd(files_with_rse)
    
    #We find all the directories in the custome data structure, so we can find the files in the storage dump that have the same directory. Currently this assumes that we only have one dataset per directory, which we might not for old test data
    directories_database=(Files_in_Rucio_data_with_rse.directory_index.keys())
    print("The directories in the custome data structure are:"+str(directories_database)+"\n")
    #We iterate over the directories in the custome data structure
    for directories_database_item in directories_database:
        print("Directory: "+directories_database_item)

        #List of files in the storage dump that have the same directory
        files_in_storage_dump=[item for item in storage_dump if directories_database_item in item]

        #list of files in the custome data structure that have the same directory
        directories_database_files_from_database=Files_in_Rucio_data_with_rse.find_by_metadata("directory",directories_database_item)
        files_from_database=[item.location for item in directories_database_files_from_database]


        #compare the two lists files and files_in_storage_dump
        
        #A cleanup step I am supprise I need to do, but I do FIXME find where this error comes from
        files_in_storage_dump=[item.replace("//","/") for item in files_in_storage_dump]

        print("NUmber of files in storage dump: "+str(len(files_in_storage_dump)))
        print("NUmber of files in custome data structure: "+str(len(files_from_database)))

        #Calculate the difference between the two lists
        #First, we find what files exist in the storage dump but not in the custome data structure
        files_in_storage_but_not_in_database=set(files_from_database)-set(files_in_storage_dump)
        #print("Files in storage but not in database:" + str(files_in_storage_but_not_in_database))
        #Second, we find what files exist in the custome data structure but not in the storage dump
        files_in_database_but_not_in_storage=set(files_in_storage_dump)-set(files_from_database)
        #print("Files in database but not in storage:" + str(files_in_database_but_not_in_storage))
        print("\n")
    return files_in_storage_but_not_in_database,files_in_database_but_not_in_storage


def extract_from_storage_dump(rse):
    txtfiles=[]
    #find all the txt files in the current directory
    for file in os.listdir("."):
        if file.endswith(".txt"):
            txtfiles.append(file)
    #choose the file that contains the rse
    files_rse=[file for file in txtfiles if rse in file]

    #we should only have a single storage dump, so we check for that
    if len(files_rse) == 0:
        print("No files for rse: "+rse+" found")
        exit(1)

    if len(files_rse) > 1:
        print("Multiple files for rse: "+rse+" found")
        exit(1)


    files_rse=files_rse[0]

    #laod the data from the file. TODO: IF the file is large I can see this beeing slow, perghaps implement a way to load it in chucks, less memory intensive as well
    with open(files_rse, 'r') as f:
        #read the lines
        readlines=f.readlines()
        f.close()

    #make into list
    readlines=[line.strip() for line in readlines]
    return readlines


#Ap0.001GeV-sim-test,Ap0.001GeV,v3.2.10_targetPN-batch1,v1.7.1_ecal_photonuclear-recon_bdt2-batch1,v1.7.1_target_gammamumu-batch30,v1.7.1_target_gammamumu_8gev_reco-batch19,v2.3.0-batch20,v2.3.0-batch34
if __name__ == "__main__":
    # Get the arguments from the command line
    args = get_args()
    print("Loading arguments:\n"+str(args))


    #check if the rse argument is valid
    if args.rse:
        #get a list of valid rses from Rucio
        valid_rses=list(RucioFunctions.list_rses())
        #extract the rse names from the list of dictionaries
        valid_rses=[item['rse'] for item in valid_rses]
        #check if the rse argument is in the list of valid rses
        if args.rse not in valid_rses:
            print(f"Error: --rse {args.rse} is not a valid rse")
            print("Valid rses are: "+str(valid_rses))
            exit(1)
    else:
        #if the rse argument is not set, we cannot continue
        print("Error: --rse is required")
        exit(1)


    #check if the number of threads argument is a whole integer and larger or equal to 1
    print("Verifying if the threads argument is valid.\n")
    if args.threads < 1 or args.threads % 1 != 0:
        print("Error: --threads must be a whole integer larger or equal to 1")
        exit(1)
    
    # Get the datasets, based on the arguments
    print("Loading datasets\n")
    datasets = get_datasets_from_args(args)
    print("Number of datasets: "+str(len(datasets))+"\n")

    #remove any spaces in the dataset names or scopes. Dont know if this even is a problem, but better safe than sorry
    datasets = [(dataset[0].replace(" ",""),dataset[1].replace(" ",""),args.rse) for dataset in datasets]

    #check if the datasets exist in the local database
    if args.localdb: #if we set the --localdb argument, we can use the local database instead of loading the data from Rucio
        #return a llist of datasets that are not in the local database and a list of datasets that are in the local database
        dataset_not_in_local_database,dataset_already_in_local_database=use_local_database(datasets,args)
    else: #if we do not set the --localdb argument, we need to load the data from Rucio
        dataset_not_in_local_database=datasets
        dataset_already_in_local_database=[]

    #We add the rse to the dataset names, as the dataset names are not unique across rses. FIXME This is a bit cluncky, could probably be added to the use_local_database fucntion
    dataset_already_in_local_database=[(item[0],item[1],args.rse) for item in dataset_already_in_local_database]
    dataset_not_in_local_database=[(item[0],item[1],args.rse) for item in dataset_not_in_local_database]

    print("Number of datasets that already exist in the local database: "+str(len(dataset_already_in_local_database)))
    print("Number of datasets to be loaded from Rucio: "+str(len(dataset_not_in_local_database))+"\n")
    #For data that already exist in local database and the number of files match, we can use the old local database data isnteaed of loading it from Rucio.
    #This is done by simply extracting the values from Rucio and putting it in the custom data structure
    
    #We create a custom data structure. This was changed from a SQlite database due to two reasons: Speed and Memory usage. The custome data structure is substantionally faster than the SQLite when running the code for a single or a few datasets, which seams to be the idea. With the SQlite database ou als oneed to query it quite a few times, which also is slow to set up each time. The custom data structure is also less memery intensive. The only advantage for the SQlite is that its good for more permanent storage, but that is not the idea here
    Files_in_Rucio_data=CustomDataStructure()
    #We add the data from the local database to the custom data structure
    if len(dataset_already_in_local_database) > 0:
        print("Loading data from the local database\n")
        #We load the data from the local database in a multithreaded way
        Data_from_datasets=(run_threads(thread_count=args.threads,function=RucioDatasetlocal.fill_data_from_local,data=dataset_already_in_local_database))
        #We combine the list of list into one list, as the run_threads function returns a list of list
        Data_from_datasets=[item for sublist in Data_from_datasets for item in sublist]
        print("Combining data from the local database\n")
        #We add the data from the local database to the custom data structure
        Files_in_Rucio_data.multiadd(Data_from_datasets)


    #For data that does not exist in the local database, we need to load it from Rucio.
    #This is done in a multithreaded way
    if len(dataset_not_in_local_database) > 0:
        print("Loading data from Rucio\n")
        #For each dataset, we load the data from Rucio in a multithreaded way
        #it was tested and running each dataset seperatly with its subfunctions multitrheaded was faster than runnning the datasets in a seperate thread 
        for dataset_not_in_local in ((dataset_not_in_local_database)):
            datastructure=RucioDatasetRucio.extract_from_rucio(dataset=dataset_not_in_local,thread_count=args.threads, rse=args.rse)

            #We add the data from Rucio to the custom data structure
            Files_in_Rucio_data.multiadd(datastructure)

    print("NUmber of files in the custom data structure: "+str(len(Files_in_Rucio_data.id_index.keys()))+"\n")
    
    print("Loading the storage dump\n")
    #We load teh stroage dump
    storage_dump=extract_from_storage_dump(args.rse)

    print("Finding the dark data\n")
    #We find the dark data
    files_in_storage_but_not_in_database,files_in_database_but_not_in_storage=find_dark_data(Files_in_Rucio_data,args.rse,storage_dump)
    print(len(files_in_storage_but_not_in_database))
    print(len(files_in_database_but_not_in_storage))

    #write to theie own txt files
    with open("files_in_storage_but_not_in_database.txt", 'w') as f:
        for item in files_in_storage_but_not_in_database:
            f.write("%s\n" % item)
        f.close()

    with open("files_in_database_but_not_in_storage.txt", 'w') as f:
        for item in files_in_database_but_not_in_storage:
            f.write("%s\n" % item)
        f.close()

    #TODO
    #add the fucntions for checking if there exist replcias we can repalce the original missign file with

    #TODO
    #add the function that checks if the file in question is a duplciate file

    #TODO
    #verification that those files are indeed missing from Rucio
    




































    """
    save_to_file="all.pkl"
    filename="time.txt"
    #delete the old files
    if os.path.isfile(save_to_file):
        os.remove(save_to_file)
    if os.path.isfile(filename):
        os.remove(filename)

    with open(save_to_file, 'wb') as f:
        pickle.dump(Files_in_Rucio_data, f)
        f.close()
    print("Saved datastructure to file: "+save_to_file)
    

    
    with open(filename, 'w') as f:
        f.write("Time to load arguments: {:.3f} seconds\n".format((loadargs - timestart).total_seconds()))
        f.write("Time to load datasets: {:.3f} seconds\n".format((loaddatasetstime - loadargs).total_seconds()))
        f.write("Time to remove spaces from the datasets: {:.3f} seconds\n".format((loadremovespaces - loaddatasetstime).total_seconds()))
        f.write("Time to load the local database: {:.3f} seconds\n".format((loadlocaldb - loadremovespaces).total_seconds()))
        f.write("Time to load the data from the local database: {:.3f} seconds\n".format((loadlocaldbdata - loadlocaldb).total_seconds()))
        f.write("Time to load the data from Rucio: {:.3f} seconds\n".format((loadfromrucio - loadlocaldbdata).total_seconds()))
        f.write("Time to combine the data from Rucio and the local database: {:.3f} seconds\n".format((loadcombinedata - loadfromrucio).total_seconds()))
        f.write("Total time: {:.3f} seconds\n".format((loadcombinedata - timestart).total_seconds()))"""