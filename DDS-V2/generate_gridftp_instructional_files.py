# This script reads the SQLite database and extracts what files Rucio believes exist at what storage facility.
# For each RSE, a file is created that contains the list of directories and files that Rucio believes exist at that RSE.

# Import libraries
import sqlite3 as sl
import os
import shutil
# Get the width of the terminal
terminal_width, _ = shutil.get_terminal_size()


def adding_data_about_replicas():
    #print a line to the terminal
    print('-' * terminal_width)
    print('-' * terminal_width)

    print("Adding data about replicas")
    # Initialize the databases
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    print("Connected to Rucio database")
    # Next, for each RSE, generate a list of directories and files that Rucio believes exist at that RSE

    print("Adding data about replicas for each table/dataset")
    # For each RSE in the table
    for table in (rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'")):
        print("\n")
        print("Adding data about replicas for table {}".format(table[0]))

        table = table[0]
        if table not in ["dataset","sqlite_sequence"]:
            #get the data from the table
            print("Getting data from table {}".format(table))
            data = rucio_database.execute("SELECT rse,location  FROM {}".format(table)).fetchall()
            print("Extracting inforamtion about RSEs and locations")
            rses = [i[0] for i in data]
            #from the data location whcih is i[1] for i in data, i want to remove everything ater the last /
            data = [(i[0], os.path.dirname(i[1]).rstrip("\\")) for i in data]

            #add this to teh table called dataset where the $table 
            rucio_database.execute("UPDATE dataset SET exist_at_rses = ?, directory = ? WHERE table_name = ?", (str(list(set(rses))), str(list(set(data))), table))
            rucio_database.commit()
            print("Added data about replicas for table {}".format(table))

    print("Added data about replicas for each table/dataset, closing database")
    rucio_database.close()
    print("Done")
