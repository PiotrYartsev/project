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



    break
    

"""
# If directory with name rse does not already exist, create it
table = table[0]
if table not in ["dataset","sqlite_sequence"]:
if not os.path.exists("RSE/"+table):
    # Create directory
    os.mkdir("RSE/"+table)
# Create file in the rse directory and name it the date and time
file_name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"
# Create file
file = open("RSE/"+table + "/" + file_name,"w")
# Write the directories from the SQLite database to the file
for row in rucio_database.execute("SELECT directory FROM " + table).fetchall():
    row = row[0]
    file.write(row + "\n")
# Close file
file.close()"""