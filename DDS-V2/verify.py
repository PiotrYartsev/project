import os
import sqlite3 as sl
import datetime
from tqdm import tqdm
#from Rucio_functions import check_files_exist

def verify(logger):
    for rse in os.listdir("/home/piotr/media/aurora-home/RSE"):
        print(rse)
        if os.path.isdir("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data"):
            #missing_from_rucio(rse)
            create_missing_from_storage(rse)
            
"""
def missing_from_rucio(rse):
    #check if the database exists
    if os.path.isfile("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db'):
        missing_from_rucio_database = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db')
        
        for table in missing_from_rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table';"):
            print(table[0])
            get_files_missing_from_rucio = missing_from_rucio_database.execute("SELECT scope,name FROM "+table[0]).fetchall()
            print(len(get_files_missing_from_rucio))
            #make a list of list where each list is 999 files long
            splitlist=[get_files_missing_from_rucio[i:i + 999] for i in range(0, len(get_files_missing_from_rucio), 999)]
        
            delete_list=[]
            for list in tqdm(splitlist): 
                value=(check_files_exist(list))
                delete_list.extend(value)
            print(len(delete_list))
            #delete the files from the database
            for file in delete_list:
                missing_from_rucio_database.execute("DELETE FROM "+table[0]+" WHERE scope=? AND name=?",file)
            missing_from_rucio_database.commit()
            #if the table is empty delete it
            if len(missing_from_rucio_database.execute("SELECT * FROM "+table[0]).fetchall())==0:
                missing_from_rucio_database.execute("DROP TABLE "+table[0])
                missing_from_rucio_database.commit()
        missing_from_rucio_database.close()
"""

def create_missing_from_storage(rse):
    #check if the database exists
    if os.path.isfile("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_storage.db'):
        #for each table, create a new text file with the missing direcoty 
        missing_from_storage_database = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_storage.db')
        for table in missing_from_storage_database.execute("SELECT name FROM sqlite_master WHERE type='table';"):
            directory_list=missing_from_storage_database.execute("SELECT directory FROM "+table[0]).fetchall()
            #if folder "/home/piotr/media/aurora-home/RSE/"+rse+"/verify/" does not exist, create it
            if not os.path.isdir("/home/piotr/media/aurora-home/RSE/"+rse+"/verify/"):
                os.mkdir("/home/piotr/media/aurora-home/RSE/"+rse+"/verify/")

            #write it to file
            file = open("/home/piotr/media/aurora-home/RSE/"+rse+"/verify/"+table[0]+".txt","w")
            for directory in directory_list:
                file.write(directory[0]+"\n")
            file.close()
        missing_from_storage_database.close()
