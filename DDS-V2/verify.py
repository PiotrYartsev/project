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
            missing_from_storage_database = sl.connect("RSE/"+rse+"/Dark_data/"+'missing_from_storage.db')
            missing_from_rucio_database = sl.connect("RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db')

            #verify missing from rucio
            files_missing_from_rucio = missing_from_rucio_database.execute("SELECT * FROM missing_from_rucio").fetchall()
