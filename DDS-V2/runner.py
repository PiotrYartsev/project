#this is the main software that runs all the other parts
from Rucio_functions import RucioFunctions

from database_from_rucio import RucioDataset, CustomDataStructure, combine_datastructures
from argument_loader import get_args, get_datasets_from_args
from multithreading import run_threads
import os
import sqlite3 as sl
import random
import datetime
import contextlib
import pickle

"""

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler and set its level to INFO
file_handler = logging.FileHandler('myapp.log')
file_handler.setLevel(logging.INFO)

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


#Get the width of the terminal and print a welcome message
terminal_width, _ = shutil.get_terminal_size()


#Get todays date, which is used for naming archives and finding the most resent storage dump
today=datetime.date.today()
today=str(today)
today=today.replace("-","_")


archives_generator(today,logger)


adding_data_about_replicas(logger)




transforming_to_database_from_txt(today,logger)


find_dark_data(logger)


verify(logger)


move_to_archives(today,logger)
"""

def use_local_database(datasets):
    if os.path.isfile('local_rucio_database.db'):
        print("Local database found, checking if the datasets exist in the local database\n")
        LocalRucioDataset=sl.connect('local_rucio_database.db')
        local_database_dataset_data=LocalRucioDataset.execute("SELECT scope, name, length FROM dataset").fetchall()

        #print(local_database_dataset_data)
        output=run_threads(thread_count=args.threads,function=RucioDataset.check_if_exist,data=datasets,const_data=local_database_dataset_data)
        list_of_dataset_not_in_local_database=[]
        list_of_dataset_already_in_local_database=[]
        print("Moving datasets to the correct list\n")
        for item in output:
            if item[1]:
                list_of_dataset_already_in_local_database.append(item[0])
            else:
                list_of_dataset_not_in_local_database.append(item[0])
    else:
        print("Local database not found, all datasets will be loaded from Rucio\n")
        list_of_dataset_not_in_local_database=datasets
        list_of_dataset_already_in_local_database=[]
    return list_of_dataset_not_in_local_database,list_of_dataset_already_in_local_database

#Ap0.001GeV-sim-test,Ap0.001GeV,v3.2.10_targetPN-batch1,v1.7.1_ecal_photonuclear-recon_bdt2-batch1,v1.7.1_target_gammamumu-batch30,v1.7.1_target_gammamumu_8gev_reco-batch19,v2.3.0-batch20,v2.3.0-batch34
if __name__ == "__main__":
    timestart=datetime.datetime.now()
    # Get the arguments from the command line
    args = get_args()
    print("Loading arguments:\n"+str(args))
    loadargs=datetime.datetime.now()

    #check if the number of threads argument is a whole integer and larger or equal to 1
    print("Verifying if the threads argument is valid.\n")
    if args.threads < 1 or args.threads % 1 != 0:
        print("Error: --threads must be a whole integer larger or equal to 1")
        exit(1)
    
    # Get the datasets, based on the arguments
    print("Loading datasets\n")
    datasets = get_datasets_from_args(args)
    loaddatasetstime=datetime.datetime.now()
    #choose 20 random datasets
    #datasets=random.sample(datasets,20)

    #remove any spaces in the dataset names or scopes
    print("Removing spaces from the dataset names and scopes\n")
    datasets = [(dataset[0].replace(" ",""),dataset[1].replace(" ","")) for dataset in datasets]
    loadremovespaces=datetime.datetime.now()


    #check if the datasets exist in the local database

    if args.localdb: #if we set the --localdb argument, we can use the local database instead of loading the data from Rucio
        list_of_dataset_not_in_local_database,list_of_dataset_already_in_local_database=use_local_database(datasets)
    else: #if we do not set the --localdb argument, we need to load the data from Rucio
        list_of_dataset_not_in_local_database=datasets
        list_of_dataset_already_in_local_database=[]
    loadlocaldb=datetime.datetime.now()


    #For data that already exist in local database and the number of files match, we can use the old local database data isnteaed of loading it from Rucio.
    #This is done by simply extracting the values from Rucio and putting it in the custom data structure
    #This is done in a multithreaded way
    Data_from_datasets_datastructure=CustomDataStructure()
    if len(list_of_dataset_already_in_local_database) > 0:
        print("Loading data from the local database\n")
        Data_from_datasets=(run_threads(thread_count=args.threads,function=RucioDataset.fill_data_from_local,data=list_of_dataset_already_in_local_database))
        Data_from_datasets=[item for sublist in Data_from_datasets for item in sublist]
        #print(Data_from_datasets)
        
        for data in Data_from_datasets:
            Data_from_datasets_datastructure.add_item(data)
    loadlocaldbdata=datetime.datetime.now()
    #combibine the list of list into one list
    #print(Data_from_datasets)
    #For data that does not exist in the local database, we need to load it from Rucio.
    #RucioFunctions.list_files_dataset
    #This is done in a multithreaded way
    Data_from_Rucio=CustomDataStructure()
    if len(list_of_dataset_not_in_local_database) > 0:
        print("Loading data from Rucio\n")
        
        for dataset_not_in_local in (list_of_dataset_not_in_local_database):
            datastructure=RucioDataset.extract_from_rucio(dataset=dataset_not_in_local,thread_count=args.threads)
    loadfromrucio=datetime.datetime.now()
    print("Combining data from Rucio and the local database\n")
    New_database=combine_datastructures(datastructure1=Data_from_Rucio,datastructure2=Data_from_datasets_datastructure)
    loadcombinedata=datetime.datetime.now()
    print(New_database.rse_index.keys())

    save_to_file="datastructure.pkl"
    with open(save_to_file, 'wb') as f:
        pickle.dump(New_database, f)
        f.close()
    print("Saved datastructure to file: "+save_to_file)
    

    filename="time.txt"
    with open(filename, 'w') as f:
        f.write("Time to load arguments: {:.3f} seconds\n".format((loadargs - timestart).total_seconds()))
        f.write("Time to load datasets: {:.3f} seconds\n".format((loaddatasetstime - loadargs).total_seconds()))
        f.write("Time to remove spaces from the datasets: {:.3f} seconds\n".format((loadremovespaces - loaddatasetstime).total_seconds()))
        f.write("Time to load the local database: {:.3f} seconds\n".format((loadlocaldb - loadremovespaces).total_seconds()))
        f.write("Time to load the data from the local database: {:.3f} seconds\n".format((loadlocaldbdata - loadlocaldb).total_seconds()))
        f.write("Time to load the data from Rucio: {:.3f} seconds\n".format((loadfromrucio - loadlocaldbdata).total_seconds()))
        f.write("Time to combine the data from Rucio and the local database: {:.3f} seconds\n".format((loadcombinedata - loadfromrucio).total_seconds()))
        f.write("Total time: {:.3f} seconds\n".format((loadcombinedata - timestart).total_seconds()))
        