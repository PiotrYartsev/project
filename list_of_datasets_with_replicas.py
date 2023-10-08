import sqlite3 as sl

LocalRucioDataset=sl.connect('local_rucio_database.db')
list_of_of=[]

for table in LocalRucioDataset.execute("SELECT name FROM sqlite_master WHERE type='table';"):
    if table[0]=="sqlite_sequence" or table[0]=="dataset":
        continue
    else:
        #print(table[0])
        #find the column has_replicas
        has_replicas=LocalRucioDataset.execute("SELECT has_replicas FROM "+table[0]+" WHERE has_replicas=1").fetchall()
        if len(has_replicas)>0:
            list_of_of.append(table[0])
datasets_to_run=[]
for dataset in list_of_of:
    #in table datasets retrive column name where table_name=dataset
    datase_name=LocalRucioDataset.execute("SELECT name,exist_at_rses FROM dataset WHERE table_name='"+dataset+"'").fetchall()
    datase_name=[i[0] for i in datase_name if "LUND_GRIDFTP" in i[1]]
    datasets_to_run.extend(datase_name)

#print(datasets_to_run)

#fimnd random datasets that have files in LUND but do not have has_replicas=1, and LUND_GRIDFTP is not in exist_at_rses
datasets=LocalRucioDataset.execute("SELECT name FROM dataset WHERE exist_at_rses LIKE '%LUND_GRIDFTP%'").fetchall()
datasets=[i[0] for i in datasets]
datasets=list(set(datasets)-set(datasets_to_run))

#randomly select 10 datasets from  the list
import random
#convert current time in seconds to integer
import time
random.seed(int(time.time()))
datasets_to_run.extend(random.sample(datasets,100))
#keep only the datasets that are in the list_allowed
#datasets_to_run=list(set(datasets_to_run).intersection(set(list_allowed)))
print(str(datasets_to_run).replace(" ",""))
