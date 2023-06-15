from Rucio_functions import *

from tqdm import tqdm
import sqlite3 as sl
import re

def reading_data_from_file(file):
    #read a txt line by line to extract the datasets
    with open('{}'.format(file)) as f:
        lines = f.readlines()

    #remove whitespace characters like `\n` at the end of each line and remove the count of files
    dataset=[]
    for line in lines:
        #split str on ,
        line=line.split(",")
        #remove the count of files
        line=line[0]
        dataset.append(line)
    return dataset

dataset=reading_data_from_file("datasets_and_numbers.txt")

data={}

def read_rucio_dataset_files_and_extract_metadata(file_data_from_dataset_output):
    file_info={}
    file_info["name"]=[]
    file_info["scope"]=[]
    file_info["adler32"]=[]
    file_info["rse"]=[]
    file_info["location"]=[]
    metadata=list_replicas(scope, file_data_from_dataset_output["name"])
    metadata=list(metadata)
    #print(len(metadata))
    metadata=metadata[0]
    #print(metadata["rses"])
    for i in range(len((metadata["rses"]))):
        
        file_info["name"].append(file_data_from_dataset_output["name"])
        file_info["scope"].append(file_data_from_dataset_output["scope"])
        file_info["adler32"].append(file_data_from_dataset_output["adler32"])
        #for key number i, so first key for i=0, second key for i=1, etc
        file_info["rse"].append(list(metadata["rses"].keys())[i])
        file_info["location"].append(metadata["rses"][list(metadata["rses"].keys())[i]])
    if len(metadata["rses"]) != 1:
            print("Error: more than one rse, be careful!")
            print(metadata["rses"])
            print(file_info)
    return file_info



for dataset_name in tqdm(dataset):

    scope, name = dataset_name.split(":")
    output=(list(list_files_dataset(scope, name)))
    data[name]=[]
    for file_data_from_dataset_output in tqdm(output[:2]):
        file_info=read_rucio_dataset_files_and_extract_metadata(file_data_from_dataset_output)
        #print(file_info)
        
#
#    
#con = sl.connect('Rucio_data_LUND_GRIDFTP.db')
##if table does not already exist, create it
#if not con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset';").fetchall():
#    con.execute("""
#        CREATE TABLE dataset (
#            name TEXT NOT NULL,
#            length INTEGER NOT NULL
#        );
#    """)
#
#
#
#for dataset in data:
#    print(dataset)
#    dataset_table_name= re.sub(r"[^a-zA-Z0-9]+", "_", dataset)
#        #if firts char is a number, add an x in front of it
#    if dataset_table_name[0].isdigit():
#            dataset_table_name="x"+dataset_table_name
#    #if dataset is not already in the table, add it
#    if con.execute("SELECT name FROM dataset WHERE name=?", (dataset,)).fetchone() is None:
#        with con:
#            con.execute("""
#                INSERT INTO dataset (name, length) VALUES (?, ?)""",
#                (dataset, len(data[dataset])))
#    
#    #if dataset_table_name is not already in the table, add it
#    if con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (dataset_table_name,)).fetchone() is None:
#        con.execute("CREATE TABLE {} (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, scope TEXT NOT NULL, rse TEXT NOT NULL, adler32 TEXT NOT NULL, timestamp INTEGER NOT NULL, filenumber INTEGER NOT NULL, location TEXT NOT NULL);".format(dataset_table_name))
#        append_to_table=[]
#        for key in data[dataset]:
#            name=key["name"]
#            scope=key["scope"]
#            adler32=key["adler32"]
#            rse=key["rse"]
#            location=key["location"]
#            location=location[rse][0]
#
#            #remove everuthing before the last _
#            namesplit=name.split("_")
#            timestamp=namesplit[-1].replace(".root","").replace("t","")
#            filenumber=namesplit[-2].replace("run","")
#            append_to_table.append((name, scope, rse, adler32, timestamp, filenumber, location))
#                    
#        sql="INSERT INTO {} (name, scope, rse, adler32, timestamp, filenumber, location) VALUES (?, ?, ?, ?, ?, ?, ?)".format(dataset_table_name)
#        with con:
#            con.executemany(sql, append_to_table)
#        count=con.execute("SELECT COUNT(*) FROM {}".format(dataset_table_name)).fetchone()
#        #compare count to length of dataset
#        if count[0] != len(data[dataset]):
#            #error
#            raise Exception("Error: count does not match length of dataset")
#        
#
#
#
##"""