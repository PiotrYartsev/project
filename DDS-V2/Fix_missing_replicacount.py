# Import necessary modules
import signal
import sqlite3 as sl
import re
from tqdm import tqdm
from Rucio_functions import list_files_dataset, list_replicas, count_files_func

# Connect to the database
con = sl.connect('/home/piotr/Documents/GitHub/project/DDS-V2/Rucio_data_LUND_GRIDFTP.db')

for table in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'"):
    if table[0] not in ["dataset","sqlite_sequence"]:
        
        #add a row to the table called has_replicas
        #con.execute("ALTER TABLE {} ADD COLUMN has_replicas INTEGER".format(table[0]))
        
        first_file = con.execute("SELECT scope,name FROM {} LIMIT 1".format(table[0])).fetchall()
        scope=first_file[0][0]
        name=first_file[0][1]
        
        replicas=list(list_replicas(scope,name))
        number = len(replicas[0]["rses"])-1
        if number != 0:
            print(table[0])
            print(number)
        #con.execute("UPDATE {} SET has_replicas = {}".format(table[0],number,scope,name))
        #con.commit()
print("done")
        
