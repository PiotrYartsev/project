# Import libraries
import sqlite3 as sl
# Library for creating directories
import os
# Library for getting the current date and time
import datetime
import sys
import ast
def generate_txt():
    import shutil
    # Get the width of the terminal
    terminal_width, _ = shutil.get_terminal_size()
    print('-' * terminal_width)
    print('-' * terminal_width)
    print("Generating text files with information about files at what RSEs to be used by the GridFTP server")
    
    # Connect to the SQLite database
    print("Connecting to Rucio database")
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    
    #get the datasets
    print("Getting datasets")
    datasets = rucio_database.execute("SELECT * FROM dataset").fetchall()

    #date day-month-year
    print("Getting date for naming the files")
    date=datetime.datetime.now().strftime("%d-%m-%Y")
    """
    for rse in os.listdir("RSE"):
        print()
        if os.path.isdir("RSE/"+rse):
            #if anything is in the directory move to archive
            if os.listdir("RSE/"+rse):
                #create archives/date
                if not os.path.exists("archives/"+date):
                    os.mkdir("archives/"+date)
                #move the files in the directory to the archive

                os.system(f"mv RSE/{rse} archives/"+date)"""

    for dataset in datasets:
        print("Generating text file for dataset {}".format(dataset[0]))
        directory = dataset[3]
        directory = ast.literal_eval(directory)
        for list in directory:
            rse=list[0]
            #print(rse)
            directory=list[1]
            #print(directory)
            if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse):
                # Create directory
                os.mkdir("/home/piotr/media/aurora-home/RSE/"+rse)
            #open the file without deleting the content
            file = open("/home/piotr/media/aurora-home/RSE/"+rse+f"/{rse}_rucio_dump_{date}.txt","a")
            #write the directory to the file
            file.write(directory + "\n")    
            #close the file
            file.close()
    print("Done")
    
        
    