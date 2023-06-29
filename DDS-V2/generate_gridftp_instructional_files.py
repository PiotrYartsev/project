# This script reads the SQLite database and extracts what files Rucio believes exist at what storage facility.
# For each RSE, a file is created that contains the list of directories and files that Rucio believes exist at that RSE.

# Import libraries
import sqlite3 as sl
import os
from tqdm import tqdm

# Initialize the databases
rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')

# Next, for each RSE, generate a list of directories and files that Rucio believes exist at that RSE

#get the number of tables in the database
number_of_tables = rucio_database.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'").fetchall()[0][0]
#set the tqdm progress bar
pbar = tqdm(total=number_of_tables)

# For each RSE in the table

for table in (rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'")):
    pbar.update(1)
    table = table[0]
    if table not in ["dataset","sqlite_sequence"]:
        data = rucio_database.execute("SELECT rse,location  FROM {}".format(table)).fetchall()
        rses = [i[0] for i in data]
        #from the data location whcih is i[1] for i in data, i want to remove everything ater the last /
        data = [(i[0], os.path.dirname(i[1]).rstrip("\\")) for i in data]

        #add this to teh table called dataset where the $table 
        rucio_database.execute("UPDATE dataset SET exist_at_rses = ?, directory = ? WHERE table_name = ?", (str(list(set(rses))), str(list(set(data))), table))
        rucio_database.commit()