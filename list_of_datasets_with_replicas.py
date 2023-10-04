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

for dataset in list_of_of:
    #in table datasets retrive column name where table_name=dataset
    datase_name=LocalRucioDataset.execute("SELECT name,exist_at_rses FROM dataset WHERE table_name='"+dataset+"'").fetchall()
    print(datase_name[0])
    #print()