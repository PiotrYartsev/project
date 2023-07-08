# Import libraries
import sqlite3 as sl
# Library for creating directories
import os
# Library for getting the current date and time
import datetime
import sys
import ast

# Connect to the SQLite database
rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')

#get the datasets
datasets = rucio_database.execute("SELECT * FROM dataset").fetchall()

#date day-month-year
date=datetime.datetime.now().strftime("%d-%m-%Y")
for rse in os.listdir("RSE"):
    print(rse)
    if os.path.isdir("RSE/"+rse):
        #if anything is in the directory move to archive
        if os.listdir("RSE/"+rse):
            #create archives/date
            if not os.path.exists("archives/"+date):
                os.mkdir("archives/"+date)
            #move the files in the directory to the archive

            os.system(f"mv RSE/{rse} archives/"+date)




for dataset in datasets:
    #print(dataset)
    directory = dataset[3]
    directory = ast.literal_eval(directory)
    for list in directory:
        rse=list[0]
        #print(rse)
        directory=list[1]
        #print(directory)
        if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse):
            # Create directory
            os.mkdir("/home/piotr/media/aurora-home/RSE/"+rse)
        #open the file without deleting the content
        file = open("/home/piotr/media/aurora-home/RSE/"+rse+f"/{rse}_rucio_dump_{date}.txt","a")
        #write the directory to the file
        file.write(directory + "\n")    
        #close the file
        file.close()
        
for dataset in rucio_database.execute("Select name from dataset").fetchall():
    #retrive the directory
    directory = rucio_database.execute("SELECT directory FROM dataset WHERE name = ?", dataset).fetchall()[0][0]
    #print(directory)
    #transofrm from str to list
    directory = ast.literal_eval(directory)
    #print(directory)
    directory_write=[a[1] for a in directory]
    #print(directory_write)
    #overwrite the directory
    rucio_database.execute("UPDATE dataset SET directory = ? WHERE name = ?", (str(directory_write), dataset[0]))
    rucio_database.commit()