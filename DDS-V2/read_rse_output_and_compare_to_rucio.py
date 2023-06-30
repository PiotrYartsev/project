#import libraries
import os
import sqlite3 as sl
import datetime
from dark_data_functions import transforming_to_database_from_txt, comparison, missing_from_storage, create_table_missing_from_storage, missing_from_rucio, create_table_missing_from_rucio


def main():
    #connect to Rucio database
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    transforming_to_database_from_txt()
    for rse in os.listdir("RSE"):
        print(rse)
        if os.path.isdir("RSE/"+rse+"/output"):
            #open the database
            storage_output_database = sl.connect("RSE/"+rse+"/output/"+'storage_output.db')

            #missing from storage
            database_missing_from_storage = sl.connect("RSE/"+rse+"/Dark_data/this_cycle/"+'missing_from_storage.db')
            
            #missing from rucio
            database_missing_from_rucio = sl.connect("RSE/"+rse+"/Dark_data/this_cycle/"+'missing_from_rucio.db')
            
            #get the list of tables
            tables = storage_output_database.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            tables = [x[0] for x in tables]
            for table in tables:
                #print("\n\n")
                print("Dataset:", table)
                files_in_storage_missing_from_rucio,files_in_rucio_missing_from_storage,files_in_rucio_and_storage=comparison(table,rucio_database,storage_output_database)
                #print("Files in rucio and storage: ",len(files_in_rucio_and_storage))
                #print("Files in storage missing from rucio: ",len(files_in_storage_missing_from_rucio))
                #print("Files in rucio missing from storage: ",len(files_in_rucio_missing_from_storage))
                
                if len(files_in_rucio_missing_from_storage)>0:
                    replicas_add_to_table,scope=missing_from_storage(table,files_in_rucio_missing_from_storage,rucio_database)
                    #replicas_add_to_table=[str(x) for x in replicas_add_to_table]
                    create_table_missing_from_storage(table=table,database_missing_from_storage=database_missing_from_storage,scope=scope,replicas_add_to_table=replicas_add_to_table,files_missing_from_storage=files_in_rucio_missing_from_storage)
                else:
                    print("No files missing from storage")

                if len(files_in_storage_missing_from_rucio)>0:
                    outouut=missing_from_rucio(files_missing_from_rucio=files_in_storage_missing_from_rucio,table=table,rucio_database=rucio_database)
                    create_table_missing_from_rucio(table,outouut,database_missing_from_rucio)
                
                

if __name__ == "__main__":
    main()