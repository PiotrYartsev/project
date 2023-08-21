# Import libraries
import sqlite3 as sl
import os
import datetime
import ast
import logging

def generate_txt(logger):
    # Connect to the SQLite database
    logger.info("Connecting to Rucio database")
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    
    # Get the datasets
    logger.info("Getting datasets")
    datasets = rucio_database.execute("SELECT * FROM dataset").fetchall()

    # Get the current date and time
    logger.info("Getting date for naming the files")
    date=datetime.datetime.now().strftime("%d-%m-%Y")

    for dataset in datasets:
        # Generate text file for each dataset
        logger.info("Generating text file for dataset {}".format(dataset[0]))
        directory = dataset[3]
        directory = ast.literal_eval(directory)
        # Check if directory is a tuple with only tuples as elements
        if isinstance(directory, tuple) and all(isinstance(item, tuple) for item in directory):
            pass
        else:
            # Convert directory to a tuple with one element
            directory = tuple([directory])
        for item in directory:
            rse=item[0]
            directory=item[1]   
            
            if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse):
                # Create directory if it does not exist
                os.mkdir("/home/piotr/media/aurora-home/RSE/"+rse)

            # Open the file without deleting the content
            file = open("/home/piotr/media/aurora-home/RSE/"+rse+f"/{rse}_rucio_dump_{date}.txt","a")
            # Write the directory to the file
            file.write(directory + "\n")    
            # Close the file
            file.close()
