#import libraries
import os
import sqlite3 as sl
import json
import re
from tqdm import tqdm
import zipfile
import shutil
terminal_width, _ = shutil.get_terminal_size()
    
def transforming_to_database_from_txt(today):
    print('_' * terminal_width)
    print("Transforming txt files to database")
    print('_' * terminal_width)
    #connect to the directories_database to get the directory to dataset mapping
    directories_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    
    #first step read all the rses in the folder /home/piotr/media/aurora-home/RSE
    #then for each rse, find the most recent folder with the format YYYY_MM_DD
    for rse in os.listdir("/home/piotr/media/aurora-home/RSE"):
        print("Reading the rse: "+rse)
        files_directory = []
        most_recent_folder = None
        most_recent_time = 0
        #find the most recent folder with the format YYYY_MM_DD
        print("Finding the most recent folder")
        for folder in os.listdir("/home/piotr/media/aurora-home/RSE/"+rse):
            if re.match(r'\d{4}_\d{2}_\d{2}', folder):
                folder_time = os.path.getmtime("/home/piotr/media/aurora-home/RSE/"+rse+"/"+folder)
                if folder_time > most_recent_time:
                    most_recent_folder = folder
                    most_recent_time = folder_time
        
        if most_recent_folder is not None:
            print("The most recent folder is: "+most_recent_folder)
            #get a list of the files in the most recent folder
            files=os.listdir("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
            #print(files)
            for file in files:
                if file.endswith(".txt"):
                    files_directory.append("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder+"/"+file)
        print("Transforming the txt files to database")
        #check if there are txt files in the output directory
        for output_file in tqdm(files_directory):
            print("Transforming the file: "+output_file)
            #read the content of the file
            with open(output_file) as f:
                file_content = f.readlines()
            file_content = [x.strip() for x in file_content]
            directory_val=file_content[0]
            file_content = file_content[1:]
            print("Finding out which dataset the file belongs to")
            directory_and_table_name=directories_database.execute("SELECT directory, table_name FROM dataset").fetchall()
            
            for directory,table_name in directory_and_table_name:
                if directory_val in directory:
                    dataset=table_name
                    print("The file belongs to the dataset: "+dataset)
                    break
           
            #if "/home/piotr/media/aurora-home/RSE/"+rse+"/output/" dow not exist create it
            if not os.path.exists("/home/piotr/media/aurora-home/RSE/"+rse+"/output/"):
                os.makedirs("/home/piotr/media/aurora-home/RSE/"+rse+"/output/")
            
            #create new database called storage_output.db
            print("Creating the database storage_output.db")
            storage_output_database = sl.connect("/home/piotr/media/aurora-home/RSE/"+rse+"/output/"+'storage_output.db')
            #create table with the name of the dataset
            print("Creating the table "+dataset)
            storage_output_database.execute("CREATE TABLE IF NOT EXISTS "+dataset+" (name TEXT not null, directory TEXT not null)")
            storage_output_database.commit()
            #insert the files in the table
            print("Inserting the files in the table "+dataset)
            for file in file_content:
                storage_output_database.execute("INSERT INTO "+dataset+" (name,directory) VALUES ('"+file+"','"+directory_val+"')")
            storage_output_database.commit()
            #close the database
            storage_output_database.close()
            
        #compress the most recent folder and move it to the /home/piotr/Documents/GitHub/project/DDS-V2/archives folder, then delete the folder
        print("Compressing the most recent folder and moving it to the archives folder")
        if most_recent_folder is not None:
            #compress the folder
            shutil.make_archive(most_recent_folder, 'zip', "/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
            #move the compressed folder to the archives folder
            shutil.move(most_recent_folder+".zip", "/home/piotr/Documents/GitHub/project/DDS-V2/archives/{}".format(today))
            #delete the original folder
            shutil.rmtree("/home/piotr/media/aurora-home/RSE/"+rse+"/"+most_recent_folder)
        
    #close the database
    directories_database.close()

        


def comparison(table,rucio_database,storage_output_database):

    #get the files in rucio
    print("Getting the files in rucio")
    rucio_files = rucio_database.execute("SELECT name, location FROM "+table).fetchall()

    scope=rucio_database.execute("SELECT scope FROM "+table).fetchall()[0][0]

    #get the files in the output database
    print("Getting the files in storage")
    output_files = storage_output_database.execute("SELECT name, directory FROM "+table).fetchall()
    output_files=[(x[0],x[1]+"/"+x[0]) for x in output_files]
    #compare the two lists
    print("Comparing the storage and rucio files")
    files_in_storage_missing_from_rucio = list(set(output_files) - set(rucio_files))
    files_in_storage_missing_from_rucio=[(scope,x[0],x[1]) for x in files_in_storage_missing_from_rucio]
    
    files_in_rucio_missing_from_storage = list(set(rucio_files) - set(output_files))
    files_in_rucio_and_storage = list(set(rucio_files) & set(output_files))

    print("Done comparing the storage and rucio files")
    return files_in_storage_missing_from_rucio,files_in_rucio_missing_from_storage,files_in_rucio_and_storage


def missing_from_storage(table,files_missing_from_storage,rucio_database):

    replicas_add_to_table=[]
    scope = rucio_database.execute(f"SELECT scope FROM {table}").fetchall()[0][0]
    print("For the files missing from storage, checking if they have replicas in rucio")
    for file_data in files_missing_from_storage:
        print("Checking if the file "+file_data[0]+" has replicas in rucio")
        file=file_data[0]
        

        directory=file_data[1]

        #chack if that file has replicas in rucio
        print("Extracting the number of replicas from rucio")
        replicas = rucio_database.execute(f"SELECT has_replicas FROM {table} WHERE name = '"+file+"' and location='"+directory+"'").fetchall()[0][0]
        #get the scopes in that tabel
        if replicas!=0:
            print("The file "+file+" has replicas in rucio")
            #find all the replicas of that file
            print("Writing the replicas of the file "+file+" to the table")
            replicas_of_that_file_add = rucio_database.execute(f"SELECT scope,name,location FROM {table} WHERE name = '"+file+"' and location is not '"+directory+"'").fetchall()
            replicas_add_to_table.append(replicas_of_that_file_add)
        else:
            print("The file "+file+" does not have replicas in rucio")
            #if there are no replicas then add the file to the table
            print("Weiting an empty list to the table")
            replicas_add_to_table.append([])
    return replicas_add_to_table,scope

def create_table_missing_from_storage(table,database_missing_from_storage, files_missing_from_storage, replicas_add_to_table, scope):
    print("Adding the files missing from storage to the database")
    # Create the table
    print("Create the table:" + table)
    database_missing_from_storage.execute(f"CREATE TABLE IF NOT EXISTS {table} (name TEXT not null, directory TEXT not null, scope TEXT not null, has_replicas TEXT not null)")
    

    # Insert the data into the table
    print("Inserting the data into the table")
    for n in range(len(files_missing_from_storage)):
        file=files_missing_from_storage[n][0]
        directory=files_missing_from_storage[n][1]
        
        replicas=replicas_add_to_table[n]
        replicas=json.dumps(replicas)
        database_missing_from_storage.execute(f"""INSERT INTO {table} (name, directory, scope, has_replicas) VALUES ('{file}', '{directory}', '{scope}', '{replicas}')""")

    # Commit the changes to the database
    database_missing_from_storage.commit()


def create_table_missing_from_rucio(table,missing_from_rucio_output,database_missing_from_rucio):
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

def missing_from_rucio(files_missing_from_rucio,table,rucio_database):
    print("Checking if the files missing from rucio are duplcate files")
    scope=[i[0] for i in files_missing_from_rucio]
    full_name=[i[2] for i in files_missing_from_rucio]

    file=[i[2].split("/")[-1] for i in files_missing_from_rucio]

    
    file_number=[i.split("_")[-2].replace("run","") for i in file]

    location=full_name[0].rsplit("/",1)[0]

    
    file_numbers_dict={}
    
    for n in range(len(file_number)):
        if file_number[n] not in file_numbers_dict:
            file_numbers_dict[file_number[n]]=[]
        file_numbers_dict[file_number[n]].append((scope[n],file[n],full_name[n],location,file_number[n]))

    output=[]
    for number in file_numbers_dict:
        files_in_rucio = rucio_database.execute(f"SELECT scope ,name, location FROM {table} WHERE filenumber={number} and location LIKE '%{location}%'").fetchall()
        files_in_rucio=[(x[0],x[1],x[2]) for x in files_in_rucio]
        for item in file_numbers_dict[number]:
            output.append([item,files_in_rucio])
    return output