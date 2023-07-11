import os
import re
import sqlite3 as sl

from dark_data_functions import transforming_to_database_from_txt, comparison, missing_from_storage, create_table_missing_from_storage, missing_from_rucio, create_table_missing_from_rucio


def find_dark_data(logger):

    #connect to Rucio database
    logger.info("Connecting to Rucio database")
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
  
    for rse in os.listdir("/home/piotr/media/aurora-home/RSE"):
        logger.info("Reading the rse: "+rse)
        if os.path.isdir("/home/piotr/media/aurora-home/RSE/"+rse+"/output"):
            logger.info("The output folder exists")
            #open the database
            logger.info("Opening the database")
            storage_output_database = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/output/"+'storage_output.db')

            #if dark data folder does not exist create it
            if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"):
                logger.info("Creating the dark data folder")
                os.makedirs("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/")
            #missing from storage
            logger.info("Creating the missing from storage database")
            database_missing_from_storage = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_storage.db')
            
            #missing from rucio
            logger.info("Creating the missing from rucio database")
            database_missing_from_rucio = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/Dark_data/"+'missing_from_rucio.db')
            
            #get the list of tables
            tables = storage_output_database.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            tables = [x[0] for x in tables]
            
            
            for table in tables:
                logger.info("Reading the table: "+table)

                logger.info("Finding dark/lost data in the table: "+table)
                files_in_storage_missing_from_rucio,files_in_rucio_missing_from_storage,files_in_rucio_and_storage=comparison(table,rucio_database,storage_output_database)

                logger.info("Results for the table: "+table)
                logger.info("Files in rucio and storage: "+str(len(files_in_rucio_and_storage)))
                logger.info("Files in storage missing from rucio: "+str(len(files_in_storage_missing_from_rucio)))
                logger.info("Files in rucio missing from storage: "+str(len(files_in_rucio_missing_from_storage)))
                
                
                if len(files_in_rucio_missing_from_storage)>0:
                    logger.info("Files missing from storage")
                    replicas_add_to_table,scope=missing_from_storage(table,files_in_rucio_missing_from_storage,rucio_database)
                    #replicas_add_to_table=[str(x) for x in replicas_add_to_table]
                    create_table_missing_from_storage(table=table,database_missing_from_storage=database_missing_from_storage,scope=scope,replicas_add_to_table=replicas_add_to_table,files_missing_from_storage=files_in_rucio_missing_from_storage)
                    logger.info("Files missing from storage added to the database")
                else:
                    logger.info("No files missing from storage")
                
                if len(files_in_storage_missing_from_rucio)>0:
                    logger.info("Files missing from rucio")
                    missing_from_rucio_output=missing_from_rucio(files_missing_from_rucio=files_in_storage_missing_from_rucio,table=table,rucio_database=rucio_database)
                    create_table_missing_from_rucio(table,missing_from_rucio_output,database_missing_from_rucio)
                else:
                    logger.info("No files missing from rucio")
                
                
                
        else:
            logger.info("The output folder does not exist")
            