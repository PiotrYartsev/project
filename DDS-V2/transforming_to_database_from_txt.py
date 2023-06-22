#import libraries
import os
import sqlite3 as sl
import datetime

def transforming_to_database_from_txt():
    #connect to the directories_database to get the directory to dataset mapping
    directories_database = sl.connect('directories_and_dataset_for_RSE.db')

    #first step read all the rses in the folder RSE
    #then for each rse, check if there is a folder called output
    for rse in os.listdir("RSE"):
        #print(rse)
        files_directory = []
        #if output directory exist then read the files
        if os.path.isdir("RSE/"+rse+"/output"):
            #get a list of the files in the output directory
            files=os.listdir("RSE/"+rse+"/output")
            #print(files)
            for file in files:
                if file.endswith(".txt"):
                    files_directory.append("RSE/"+rse+"/output/"+file)
        
        #check if there are txt files in the output directory
        for output_file in files_directory:
            #read the content of the file
            with open(output_file) as f:
                file_content = f.readlines()
            file_content = [x.strip() for x in file_content]
            directory=file_content[0]
            file_content = file_content[1:]
            #print(directory)
            #remove whitespace characters like `\n` at the end of each line
            
            #check what the dataset is for the directoryu in teh directories_database 
            dataset=directories_database.execute("SELECT datasets FROM "+rse+" WHERE directory = '"+directory+"'").fetchall()[0][0]
            #print(dataset)
            #create new database called storage_output.db
            storage_output_database = sl.connect("RSE/"+rse+"/output/"+'storage_output.db')
            #create table with the name of the dataset
            storage_output_database.execute("CREATE TABLE IF NOT EXISTS "+dataset+" (name TEXT not null)")
            storage_output_database.commit()
            #insert the files in the table
            for file in file_content:
                storage_output_database.execute("INSERT INTO "+dataset+" (name) VALUES ('"+file+"')")
            storage_output_database.commit()
            #close the database
            storage_output_database.close()
            
        #delete att the txt files in the output directory
        for file in files_directory:
            os.remove(file)

    
def comparison(table,rucio_database,storage_output_database):
    #get the files in rucio
    rucio_files = rucio_database.execute("SELECT name FROM "+table).fetchall()
    rucio_files = [x[0] for x in rucio_files]
    #get the files in the output database
    output_files = storage_output_database.execute("SELECT name FROM "+table).fetchall()
    output_files = [x[0] for x in output_files]
    #compare the two lists
    files_in_storage_missing_from_rucio = list(set(output_files) - set(rucio_files))
    files_in_rucio_missing_from_storage = list(set(rucio_files) - set(output_files))
    return files_in_storage_missing_from_rucio,files_in_rucio_missing_from_storage