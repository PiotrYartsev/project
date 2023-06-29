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

time=datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
for dataset in datasets:
    #print(dataset)
    directory = dataset[3]
    directory = ast.literal_eval(directory)
    for list in directory:
        rse=list[0]
        print(rse)
        directory=list[1]
        print(directory)
        if not os.path.exists("RSE/"+rse):
            # Create directory
            os.mkdir("RSE/"+rse)
        #open the file without deleting the content
        file = open("RSE/"+rse+"/"+time+".txt","a")
        #write the directory to the file
        file.write(directory + "\n")    
        #close the file
        file.close()
        
        