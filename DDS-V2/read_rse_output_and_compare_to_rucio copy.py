#import libraries
import os
import sqlite3 as sl
import datetime
from transforming_to_database_from_txt import transforming_to_database_from_txt, comparison


def main():
    #connect to Rucio database
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    transforming_to_database_from_txt()
    for rse in os.listdir("RSE"):
        print(rse)
        if os.path.isdir("RSE/"+rse+"/output"):
            #open the database
            storage_output_database = sl.connect("RSE/"+rse+"/output/"+'storage_output.db')
            #get the list of tables
            tables = storage_output_database.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            tables = [x[0] for x in tables]
            for table in tables:
                print(table)
                files_in_storage_missing_from_rucio,files_in_rucio_missing_from_storage=comparison(table,rucio_database,storage_output_database)
                print("files in storage missing from rucio: ",len(files_in_storage_missing_from_rucio))
                print("files in rucio missing from storage: ",len(files_in_rucio_missing_from_storage))

                

if __name__ == "__main__":
    main()