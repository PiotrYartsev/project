# Import necessary modules
import signal
import sqlite3 as sl
import re
from tqdm import tqdm
from Rucio_functions import list_files_dataset, list_replicas, count_files_func,list_scopes,list_dataset,list_replicas_batch
import sys
import traceback
import os

# Connect to the database
con = sl.connect('Rucio_data_LUND_GRIDFTP.db')

# Flag to indicate if the script should exit
should_exit = False

# Function to read Rucio dataset files and extract metadata
def read_rucio_dataset_files_and_extract_metadata(file_data_from_dataset_output, scope):
    file_list = [(file_data["scope"], file_data["name"]) for file_data in file_data_from_dataset_output]
    replicas = list_replicas_batch(file_list)
    data = []
    for replica in replicas:
        #print(replica)
        replicas_count = len(replica["rses"])
        for j in range(replicas_count):
            file_info = {}
            file_info["name"] = []
            file_info["scope"] = []
            file_info["adler32"] = []
            file_info["rse"] = []
            file_info["location"] = []
            file_info["has_replicas"] = []
            file_info["name"].append(replica["name"])
            file_info["scope"].append(replica["scope"])
            file_info["adler32"].append(replica["adler32"])
            file_info["rse"].append(list(replica["rses"].keys())[j])
            file_info["location"].append(replica["rses"][list(replica["rses"].keys())[j]][0])
            file_info["has_replicas"].append(replicas_count-1)
            data.append(file_info)
    return data



# Function to create a table in the database
def create_table(dataset_table_name):
    con.execute("CREATE TABLE {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, scope TEXT NOT NULL, rse TEXT NOT NULL, adler32 TEXT NOT NULL, timestamp INTEGER NOT NULL, filenumber INTEGER NOT NULL, location TEXT NOT NULL, has_replicas INTEGER NOT NULL);".format(dataset_table_name))

# Function to append data to the database table
def append_to_table(data):
    append_to_table_output=[]
    for key in data:
        name_all=key["name"]
        scope_all=key["scope"]
        adler32_all=key["adler32"]
        rse_all=key["rse"]
        location_all=key["location"]
        has_replicas_all=key["has_replicas"]
        for i in range(len(name_all)):
            name=name_all[i]
            scope=scope_all[i]
            adler32=adler32_all[i]
            rse=rse_all[i]
            location=location_all[i]
            has_replicas=has_replicas_all[i]
            namesplit=name.split("_")
            timestamp=namesplit[-1].replace(".root","").replace("t","")
            filenumber=namesplit[-2].replace("run","")
            append_to_table_output.append((name, scope, rse, adler32, timestamp, filenumber, location,has_replicas))
            
    return append_to_table_output

# Function to write data to the database table
def write_to_table(dataset_table_name,append_to_table_input,length):

    sql="INSERT INTO {} (name, scope, rse, adler32, timestamp, filenumber, location, has_replicas) VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(dataset_table_name)
    with con:
        con.executemany(sql, append_to_table_input)
    count=con.execute("SELECT COUNT(*) FROM {}".format(dataset_table_name)).fetchone()
    # Compare count to length of dataset
    if count[0] != length:
        # Error
        raise Exception("Error: count does not match length of dataset")

# Main function
def extract_from_rucio():
    #get scopes
    scopes=list_scopes()
    dataset=[]
    # Loop through each scope
    for scope in scopes:
        #if test of validation not part or subpart of scope
        if "test" not in scope and "vaidation" not in scope and "." not in scope and "validation" not in scope:
            print(scope)
            output=list_dataset(scope)
            output=[scope+":"+name for name in output]
            #get datasets
            dataset.extend(output)
    
     # If table does not already exist, create it
    if not con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset';").fetchall():
        con.execute("""
            CREATE TABLE dataset (
                name TEXT NOT NULL,
                scope TEXT NOT NULL,
                table_name TEXT NOT NULL,
                directory TEXT,
                exist_at_rses TEXT,
                length INTEGER NOT NULL
            );""")

    # Loop through each dataset
    for dataset_name in (dataset):
        
        if should_exit:
            print("Exiting due to interrupt signal...")
            break
        else:
            try:

                # Split the dataset name into scope and name
                scope,name  = dataset_name.split(":")
                # Create a table name from the dataset name
                dataset_table_name= re.sub(r"[^a-zA-Z0-9]+", "_", dataset_name)
                # If first char is a number, add an x in front of it
                if dataset_table_name[0].isdigit():
                        dataset_table_name="x"+dataset_table_name
                print(dataset_table_name)
                
                
                # If dataset_name and scope is already in the table named dataset, skip it
                if con.execute("SELECT name FROM dataset WHERE name='{}' AND scope='{}'".format(name,scope)).fetchall():
                    print("                 The dataset "+name+" is already in the table")
                    # Compare count to length of dataset in the table and the number of files in the dataset in rucio
                    number_in_table=con.execute("SELECT length FROM dataset WHERE name='{}' AND scope='{}'".format(name,scope)).fetchone()
                    number_in_table=number_in_table[0]
                    number_in_rucio=count_files_func(scope, name)
                    if number_in_table != number_in_rucio[0]:
                        print(scope,name)
                        print(number_in_rucio[0])
                        print(number_in_table)
                        print(number_in_rucio)
                        raise Exception("Error: number of files in table does not match number of files in rucio")
                    else:
                        print("                 The number of files in the table matches the number of files in rucio, skipping...")
                        
                else:
                    print("                 The dataset "+dataset_name+" is not in the table")
                    scope, name = dataset_name.split(":")
                    output=(list(list_files_dataset(scope, name)))
                    data=[]
                    
                    iterate_list=[output[i:i + 999] for i in range(0, len(output), 999)]
                    for file_data_from_dataset_list in tqdm(iterate_list):
                        #print(file_data_from_dataset_output)
                        file_info=read_rucio_dataset_files_and_extract_metadata(file_data_from_dataset_list,scope)
                        data.extend(file_info)
                    
                    length=0
                    for i in range(len(data)):
                        length+=len(data[i]["name"])

                    # If dataset is not already in the table, add it
                    con.execute("INSERT INTO dataset (scope,name,table_name,length) VALUES (?,?, ?,?)", (scope,name,dataset_table_name, length))

                    append_to_table_input=[]
                    append_to_table_output=(append_to_table(data))
                    append_to_table_input.extend(append_to_table_output)
                    create_table(dataset_table_name)
                    write_to_table(dataset_table_name,append_to_table_input,length)
            except Exception as e:
                raise Exception("An error occurred:", str(e))

    # Cleanup code or any final actions before exiting
    print("Exiting the script...")
