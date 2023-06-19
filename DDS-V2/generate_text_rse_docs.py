# Import libraries
import sqlite3 as sl
# Library for creating directories
import os
# Library for getting the current date and time
import datetime
import sys

# Connect to the SQLite database
directories_database = sl.connect('directories_and_dataset_for_RSE.db')

#if directory RSE does not exist, create it
if not os.path.exists("RSE"):
    # Create directory
    os.mkdir("RSE")
# For each RSE in the table
for table in directories_database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'"):
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
        for row in directories_database.execute("SELECT directory FROM " + table).fetchall():
            row = row[0]
            file.write(row + "\n")
        # Close file
        file.close()