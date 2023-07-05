#import libraries
import os
import sqlite3 as sl
import datetime
from tqdm import tqdm
from Rucio_functions import check_files_exist

def main():
    for rse in os.listdir("RSE"):
        print(rse)
        if os.path.isdir("RSE/"+rse+"/Dark_data"):
            missing_from_rucio(rse)
            break
            


def missing_from_rucio(rse):
    missing_from_rucio_database = sl.connect("RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db')
    for table in missing_from_rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(table[0])
        get_files_missing_from_rucio = missing_from_rucio_database.execute("SELECT scope,name FROM "+table[0]).fetchall()
        #make a list of list where each list is 999 files long
        splitlist=[get_files_missing_from_rucio[i:i + 999] for i in range(0, len(get_files_missing_from_rucio), 999)]
    
        delete_list=[]
        for list in splitlist:
            value=(check_files_exist(list))
            delete_list.extend(value)
        print(delete_list)


if __name__ == "__main__":
    main()