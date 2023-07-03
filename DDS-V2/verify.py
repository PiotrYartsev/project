#import libraries
import os
import sqlite3 as sl
import datetime
from Rucio_functions import check_file_exists

def main():
    #connect to Rucio database
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    for rse in os.listdir("RSE"):
        print(rse)
        if os.path.isdir("RSE/"+rse+"/Dark_data"):
            missing_from_rucio(rse)
            break
            


def missing_from_rucio(rse):
    missing_from_rucio_database = sl.connect("RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db')
    for table in missing_from_rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(table[0])
        get_files_missing_from_rucio = missing_from_rucio_database.execute("SELECT * FROM "+table[0])
        print(get_files_missing_from_rucio.fetchall()[0])
        break


if __name__ == "__main__":
    main()