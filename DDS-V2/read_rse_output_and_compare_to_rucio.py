#import libraries
import os
import sqlite3 as sl

#connect to the SQLite directory-dataset database
directories_database = sl.connect('directories_and_dataset_for_RSE.db')

#connect to the SQLite rucio database
rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')

#for subdirectory in RSE,not files, which is located in the same directory as this script
for rse in os.listdir(os.getcwd()+"/RSE"):
    print(rse)
    #for output directory exiist in subdirectory
    if os.path.isdir(os.getcwd()+"/RSE/"+rse+"/output"):
        #for file in output directory
        for file in os.listdir(os.getcwd()+"/RSE/"+rse+"/output"):
            #in each file, read the content
            with open(os.getcwd()+"/RSE/"+rse+"/output/"+file) as f:
                file_content = f.readlines()
            #remove whitespace characters like `\n` at the end of each line
            file_content = [x.strip() for x in file_content]
            files=file_content[1:]
            directory_storage=file_content[0]
            #check if in table rse, there is a row with the same content as the first line of the file
            datasets_and_directories=directories_database.execute("SELECT datasets,exist_at_rses FROM "+rse+" WHERE directory = '"+directory_storage+"'").fetchall()
            dataset=datasets_and_directories[0][0]
            exist_at_rses=datasets_and_directories[0][1]
            print(file)
            print(dataset)
            print(exist_at_rses)
            #in the table rucio_database named the same as the dataset, get the file names
            files_in_rucio = rucio_database.execute("SELECT name FROM {} WHERE rse = '{}'".format(dataset, rse)).fetchall()
            print(len(files_in_rucio))
            print(len(file_content)-1)
            #compare the two list and give me the files in list 1 that are not in list 2 and vice versa
            print(list(set(files_in_rucio) - set(files))[:10])
            #print(list(set(files) - set(files_in_rucio)))
            break
