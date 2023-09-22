import sqlite3 as sl

database=sl.connect("local_rucio_database.db")

for table in (database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'")):
    if table[0] not in ["dataset","sqlite_sequence"]:
        print(table[0])
        rse=database.execute("SELECT rse FROM {}".format(table[0])).fetchall()
        rse=[i[0] for i in rse]
        print(list(set(rse)))
        location_and_name=database.execute("SELECT location,name FROM {}".format(table[0])).fetchall()
        #remove /name from each location
        directory=[locatation.replace("/"+name,"") for locatation,name in location_and_name]
        print(list(set(directory)))
        #to the table dataset add the rses and the directories where the table_name matches the table
        database.execute("UPDATE dataset SET exist_at_rses = ?, directory = ? WHERE table_name = ?", (str(list(set(rse))),str(list(set(directory))),table[0]))
        database.commit()
print("done")