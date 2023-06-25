import sqlite3 as sl
import os

rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
#open txt fil called replicas.txt
f=open("replicas.txt","w+")
for table in rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall():
    table = table[0]
    if table not in ["dataset","sqlite_sequence"]:
        #cheack if any of the has_replcias values are not 0
        if rucio_database.execute("SELECT COUNT(*) FROM {} WHERE has_replicas != 0".format(table)).fetchall()[0][0] != 0:
            #write the table name to the file
            f.write(table+"\n")

f.close()
            
