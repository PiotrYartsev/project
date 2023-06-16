#The purpose of this code is to read the SQLite database and extract what files Rucio belives exist at what storage facility.
#For each RSE, a file is created that contains the list of directories and files that Rucio believes exist at that RSE.

# Import libraries
import sqlite3 as sl
import os
import sys
import argparse
import subprocess

#Initialize the database
database_access = sl.connect('Rucio_data_LUND_GRIDFTP.db')
database_for_directoreis_and_dataset_assiciated = sl.connect('directories_and_dataset_for_RSE.db')



append_to_table_input=[]
#Next, for each RSE, generate a list of directories and files that Rucio believes exist at that RSE
#for each RSE in the table
for table in database_access.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'"):
    table=table[0]
    if table not in ["dataset","sqlite_sequence"]:
        data = database_access.execute("SELECT location, rse FROM {}".format(table)).fetchall()
        location_from_table=[i[0] for i in data]
        rse_from_table=[i[1] for i in data]
        rses_many_locations=""
        if len(list(set(rse_from_table)))>1:
            rses_many_locations=list(set(rse_from_table))

        for i in range(len(data)):
            #print(data[i])
            location_from_table=(data[i][0])
            #remove everything after the last slash but keep the string
            location_from_table=location_from_table.rsplit('/', 1)[0]
            rse_from_table=(data[i][1])
            #if there is not a table in database_for_directoreis_and_dataset_assiciated that is a rse_from_table, create it
            if rse_from_table not in [i[0] for i in database_for_directoreis_and_dataset_assiciated.execute("SELECT name FROM sqlite_master WHERE type='table'")]:
                print("Creating table for {}".format(rse_from_table))
                create_table = "CREATE TABLE IF NOT EXISTS {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, directory TEXT NOT NULL, datasets TEXT NOT NULL, exist_at_rses TEXT NOT NULL)"
                database_for_directoreis_and_dataset_assiciated.execute(create_table.format(rse_from_table))
            if location_from_table not in [i[0] for i in database_for_directoreis_and_dataset_assiciated.execute("SELECT directory FROM {}".format(rse_from_table)).fetchall()]:
                print("Inserting {} into {}".format(location_from_table, rse_from_table))
                insert_into_table = "INSERT INTO {} (directory, datasets, exist_at_rses) VALUES (?, ?, ?)".format(rse_from_table)
                database_for_directoreis_and_dataset_assiciated.execute(insert_into_table, (location_from_table, str(table), str(rses_many_locations)))
                database_for_directoreis_and_dataset_assiciated.commit()
                            

    

