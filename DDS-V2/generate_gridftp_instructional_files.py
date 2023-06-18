# This script reads the SQLite database and extracts what files Rucio believes exist at what storage facility.
# For each RSE, a file is created that contains the list of directories and files that Rucio believes exist at that RSE.

# Import libraries
import sqlite3 as sl

import subprocess

# Initialize the databases
rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
#delete the old directories_database
subprocess.run(["rm", "directories_and_dataset_for_RSE.db"])
directories_database = sl.connect('directories_and_dataset_for_RSE.db')

# Next, for each RSE, generate a list of directories and files that Rucio believes exist at that RSE
# For each RSE in the table
for table in rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'"):
    table = table[0]
    if table not in ["dataset","sqlite_sequence"]:
        data = rucio_database.execute("SELECT location, rse FROM {}".format(table)).fetchall()
        locations = [i[0] for i in data]
        rses = [i[1] for i in data]
        rses_many_locations = ""
        if len(list(set(rses))) > 1:
            rses_many_locations = list(set(rses))

        for i in range(len(data)):
            location = data[i][0]
            # Remove everything after the last slash but keep the string
            location = location.rsplit('/', 1)[0]
            rse = data[i][1]
            # If there is not a table in directories_database that is a rse, create it
            if rse not in [i[0] for i in directories_database.execute("SELECT name FROM sqlite_master WHERE type='table'")]:
                create_table = "CREATE TABLE IF NOT EXISTS {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, directory TEXT NOT NULL, datasets TEXT NOT NULL, exist_at_rses TEXT NOT NULL)"
                directories_database.execute(create_table.format(rse))
            if location not in [i[0] for i in directories_database.execute("SELECT directory FROM {}".format(rse)).fetchall()]:
                insert_into_table = "INSERT INTO {} (directory, datasets, exist_at_rses) VALUES (?, ?, ?)".format(rse)
                directories_database.execute(insert_into_table, (location, str(table), str(rses_many_locations)))
                directories_database.commit()