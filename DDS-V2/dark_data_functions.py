#import libraries
import os
import sqlite3 as sl
import json
import re
import logging
import shutil


def transforming_to_database_from_txt(today,logger):
    # Log that the function is starting
    logger.info("Transforming txt files to database")
    
    # Connect to the database
    directories_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    
    # Loop through each RSE directory
    for rse in os.listdir("/home/piotr/media/aurora-home/RSE"):
        # Log which RSE directory is being read
        logger.info("Reading the rse: "+rse)
        
        # Initialize variables for finding the most recent folder
        files_directory = []
        most_recent_folder = None
        most_recent_time = 0
        
        # Find the most recent folder in the RSE directory
        logger.info("Finding the most recent folder")
        for folder in os.listdir("/home/piotr/media/aurora-home/RSE/"+rse):
            if re.match(r'\d{4}_\d{2}_\d{2}', folder):
                folder_time = os.path.getmtime("/home/piotr/media/aurora-home/RSE/"+rse+"/"+folder)
                if folder_time > most_recent_time:
                    most_recent_folder = folder
                    most_recent_time = folder_time
        
        # If there is a most recent folder, process the files in it
        if most_recent_folder is not None:
            # Log the most recent folder
            logger.info("The most recent folder is: "+most_recent_folder)
            
            # Get a list of all the .txt files in the most recent folder
            files=os.listdir("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
            for file in files:
                if file.endswith(".txt"):
                    files_directory.append("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder+"/"+file)
            
            # Process each .txt file
            logger.info("Transforming the txt files to database")
            for output_file in (files_directory):
                # Log which file is being processed
                logger.info("Transforming the file: "+output_file)
                
                # Read the contents of the file
                with open(output_file) as f:
                    file_content = f.readlines()
                file_content = [x.strip() for x in file_content]
                
                # Get the directory value from the first line of the file
                directory_val=file_content[0]
                file_content = file_content[1:]
                
                # Find which dataset the file belongs to
                logger.info("Finding out which dataset the file belongs to")
                directory_and_table_name=directories_database.execute("SELECT directory, table_name FROM dataset").fetchall()
                
                for directory,table_name in directory_and_table_name:
                    if directory_val in directory:
                        dataset=table_name
                        logger.info("The file belongs to the dataset: "+dataset)
                        break
            
                # Create the output directory if it doesn't exist
                if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse+"/output/"):
                    os.makedirs("/home/piotr/media/aurora-home/RSE/"+rse+"/output/")
                
                # Connect to the output database
                logger.info("Creating the database storage_output.db")
                storage_output_database = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/output/"+'storage_output.db')
                
                # Create the table for the dataset if it doesn't exist
                logger.info("Creating the table "+dataset)
                storage_output_database.execute("CREATE TABLE IF NOT EXISTS "+dataset+" (name TEXT not null, directory TEXT not null)")
                storage_output_database.commit()
                
                # Insert the files into the table
                logger.info("Inserting the files in the table "+dataset)
                for file in file_content:
                    storage_output_database.execute("INSERT INTO "+dataset+" (name,directory) VALUES ('"+file+"','"+directory_val+"')")
                storage_output_database.commit()
                storage_output_database.close()
        else:
            # Log that there is no folder in the RSE directory
            logger.info("There is no folder in the rse: "+rse)    
        
        # If there is a most recent folder, compress it and move it to the archives folder
        if most_recent_folder is not None:
            # Compress the most recent folder
            logger.info("Compressing the most recent folder and moving it to the archives folder")
            shutil.make_archive(most_recent_folder, 'zip', "/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
            
            # Move the zip file to the archives folder
            logger.info("Moving the zip file to the archives folder")
            shutil.move(most_recent_folder+".zip", "/home/piotr/Documents/GitHub/project/DDS-V2/archives/{}".format(today))
            
            # Delete the most recent folder and its contents
            logger.info("Deleting the most recent folder")
            shutil.rmtree("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
        
    # Close the database connection
    directories_database.close()


def comparison(table, rucio_database, storage_output_database):
    # Get the logger for this function
    logger = logging.getLogger(__name__)

    # Get the files in rucio
    logger.info("Getting the files in rucio")
    rucio_files = rucio_database.execute("SELECT name, location FROM "+table).fetchall()

    # Get the scope of the table
    scope = rucio_database.execute("SELECT scope FROM "+table).fetchall()[0][0]

    # Get the files in storage
    logger.info("Getting the files in storage")
    output_files = storage_output_database.execute("SELECT name, directory FROM "+table).fetchall()
    output_files = [(x[0], x[1]+"/"+x[0]) for x in output_files]

    # Compare the storage and rucio files
    logger.info("Comparing the storage and rucio files")
    files_in_storage_missing_from_rucio = list(set(output_files) - set(rucio_files))
    files_in_storage_missing_from_rucio = [(scope, x[0], x[1]) for x in files_in_storage_missing_from_rucio]

    files_in_rucio_missing_from_storage = list(set(rucio_files) - set(output_files))
    files_in_rucio_and_storage = list(set(rucio_files) & set(output_files))

    logger.info("Done comparing the storage and rucio files")
    return files_in_storage_missing_from_rucio, files_in_rucio_missing_from_storage, files_in_rucio_and_storage


def missing_from_storage(table, files_missing_from_storage, rucio_database):
    # Get the logger for this function
    logger = logging.getLogger(__name__)

    # Initialize the list of replicas to add to the table
    replicas_add_to_table = []

    # Get the scope of the table
    scope = rucio_database.execute(f"SELECT scope FROM {table}").fetchall()[0][0]

    logger.info("For the files missing from storage, checking if they have replicas in rucio")
    for file_data in files_missing_from_storage:
        # Check if the file has replicas in rucio
        logger.info("Checking if the file "+file_data[0]+" has replicas in rucio")
        file = file_data[0]
        directory = file_data[1]

        # Extract the number of replicas from rucio
        logger.info("Extracting the number of replicas from rucio")
        replicas = rucio_database.execute(f"SELECT has_replicas FROM {table} WHERE name = '"+file+"' and location='"+directory+"'").fetchall()[0][0]

        # Get the scopes in that table
        if replicas != 0:
            logger.info("The file "+file+" has replicas in rucio")
            # Find all the replicas of that file
            logger.info("Writing the replicas of the file "+file+" to the table")
            replicas_of_that_file_add = rucio_database.execute(f"SELECT scope,name,location FROM {table} WHERE name = '"+file+"' and location is not '"+directory+"'").fetchall()
            replicas_add_to_table.append(replicas_of_that_file_add)
        else:
            logger.info("The file "+file+" does not have replicas in rucio")
            # If there are no replicas then add the file to the table
            logger.info("Writing an empty list to the table")
            replicas_add_to_table.append([])
    return replicas_add_to_table, scope

def create_table_missing_from_storage(table, database_missing_from_storage, files_missing_from_storage, replicas_add_to_table, scope):
    logger = logging.getLogger(__name__)
    logger.info("Adding the files missing from storage to the database")
    # Create the table
    logger.info("Create the table: " + table)
    database_missing_from_storage.execute(f"CREATE TABLE IF NOT EXISTS {table} (name TEXT not null, directory TEXT not null, scope TEXT not null, has_replicas TEXT not null)")

    # Insert the data into the table
    logger.info("Inserting the data into the table")
    for n in range(len(files_missing_from_storage)):
        file = files_missing_from_storage[n][0]
        directory = files_missing_from_storage[n][1]
        replicas = replicas_add_to_table[n]
        replicas = json.dumps(replicas)
        database_missing_from_storage.execute(f"""INSERT INTO {table} (name, directory, scope, has_replicas) VALUES ('{file}', '{directory}', '{scope}', '{replicas}')""")

    # Commit the changes to the database
    database_missing_from_storage.commit()


def create_table_missing_from_rucio(table, missing_from_rucio_output, database_missing_from_rucio):
    logger = logging.getLogger(__name__)
    logger.info("Creating table for missing files from Rucio")
    # Create the table
    database_missing_from_rucio.execute(f"CREATE TABLE IF NOT EXISTS {table} (scope TEXT not null, name TEXT not null, location TEXT not null, directory TEXT not null, filenumber integer not null, replicas TEXT)")
    query = "INSERT INTO {table} (scope, name, location, directory, filenumber, replicas) VALUES (?, ?, ?, ?, ?, ?)"
    for item in missing_from_rucio_output:
        subitem = item[0]
        has_replicas = item[1]
        scope = str(subitem[0])
        file = str(subitem[1])
        location = str(subitem[2])
        directory = str(subitem[3])
        filenumber = int(subitem[4])
        if has_replicas is None:
            has_replicas = []
        values = (scope, file, location, directory, filenumber, str(has_replicas))
        database_missing_from_rucio.execute(query.format(table=table), values)
    database_missing_from_rucio.commit()
    logger.info("Finished creating table for missing files from Rucio")

def missing_from_rucio(files_missing_from_rucio, table, rucio_database):
    # Get a logger object for this module
    logger = logging.getLogger(__name__)
    # Log a message to indicate that we're checking for duplicate files
    logger.info("Checking if the files missing from Rucio are duplicate files")
    
    # Extract the scope, full name, file name, and file number for each file
    scope = [i[0] for i in files_missing_from_rucio]
    full_name = [i[2] for i in files_missing_from_rucio]
    file = [i[2].split("/")[-1] for i in files_missing_from_rucio]
    file_number = [i.split("_")[-2].replace("run", "") for i in file]
    
    # Get the location of the first file in the list
    location = full_name[0].rsplit("/", 1)[0]
    
    # Create a dictionary to group files by file number
    file_numbers_dict = {}
    for n in range(len(file_number)):
        if file_number[n] not in file_numbers_dict:
            file_numbers_dict[file_number[n]] = []
        file_numbers_dict[file_number[n]].append((scope[n], file[n], full_name[n], location, file_number[n]))
    
    # Check each group of files for duplicates in Rucio
    output = []
    for number in file_numbers_dict:
        # Construct a query to find files in Rucio with the same file number and location
        query = f"SELECT scope, name, location FROM {table} WHERE filenumber=? and location LIKE ?"
        files_in_rucio = rucio_database.execute(query, (number, f"%{location}%")).fetchall()
        files_in_rucio = [(x[0], x[1], x[2]) for x in files_in_rucio]
        # Append a list of the missing files and the matching files in Rucio to the output list
        for item in file_numbers_dict[number]:
            output.append([item, files_in_rucio])
    
    # Log a message to indicate that we're finished checking for duplicates
    logger.info("Finished checking for duplicate files in Rucio")
    return output