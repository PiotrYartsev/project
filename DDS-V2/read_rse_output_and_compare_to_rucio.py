#import libraries
import os
import sqlite3 as sl
import datetime





directories_database = sl.connect('directories_and_dataset_for_RSE.db')

#connect to the SQLite rucio database
rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')




def move_and_create_dark_data_databases(rse):
    directory_this = os.path.join(os.getcwd(), 'RSE',rse, 'Dark_data', 'this_cycle')
    directory_last = os.path.join(os.getcwd(), 'RSE',rse, 'Dark_data', 'last_cycle')
    directory_archive = os.path.join(os.getcwd(), 'RSE',rse, 'Dark_data', 'archives')

    today = datetime.date.today()

    for db_file in os.listdir(directory_this):
        # check if the file ends with .db
        if db_file.endswith('.db'):
            # split the file name into parts using underscores
            parts = db_file.split('_')
            # check if the first part is a valid date string
            try:
                file_date = datetime.datetime.strptime(parts[0], '%Y-%m-%d').date()
            except ValueError:
                # skip this file if the date string is not valid
                continue
            # check if the file date is earlier than today's date
            if file_date < today:
                print(f"         Found old file {db_file} in {directory_this}, moving to {directory_last}")
                # move the file to the last_cycle folder
                old_path = os.path.join(directory_this, db_file)
                new_path = os.path.join(directory_last, db_file)
                try:
                    os.rename(old_path, new_path)
                    print(f"Moved file {db_file} from {old_path} to {new_path}")
                except FileNotFoundError:
                    print(f"Directory {directory_last} does not exist")
                except PermissionError:
                    print(f"File {old_path} is open in another program or process")
            else:
                print(f"         Found file {db_file} in {directory_this}, deleting")
                # delete the file
                old_path = os.path.join(directory_this, db_file)
                try:
                    os.remove(old_path)
                    print(f"         Deleted file {db_file} from {old_path}")
                except FileNotFoundError:
                    print(f"File {old_path} not found")
                except PermissionError:
                    print(f"File {old_path} is open in another program or process")
        break

    # create new databases for files in storage but not in rucio
    missing_from_rucio_database = sl.connect(os.path.join(directory_this, today.strftime("%Y-%m-%d") + '_not_in_rucio.db'))

   

    # create new databases for files in rucio but not in storage
    missing_from_storage_database = sl.connect(os.path.join(directory_this, today.strftime("%Y-%m-%d") + '_not_in_storage.db'))
    return missing_from_rucio_database,missing_from_storage_database

    

def create_table(dataset,database):
    #create a table in the database
    database.execute("CREATE TABLE IF NOT EXISTS "+dataset+" (scope text not null, name text not null, location text not null, replicas text not null)")
    database.commit()




def fill_database_missing_rucio(dataset,database,files_missing_rucio_but_exist_in_storage):
    #fill the database with the files missing in rucio but exist in storage
    #get first scope from the table rucio
    scope = rucio_database.execute("SELECT scope FROM "+dataset).fetchall()[0][0]
    for file in files_missing_rucio_but_exist_in_storage:
        database.execute("INSERT INTO "+dataset+" (scope, name, replicas) VALUES ('"+scope+"', '"+file+"', '[]')")
    database.commit()


def fill_database_missing_storage(rse,dataset,database,files_missing_storage_but_exist_in_rucio):
    create_table(dataset,database)
    #fill the database with the files missing in storage but exist in rucio
    for file in files_missing_storage_but_exist_in_rucio:
        print(file)
        scope = rucio_database.execute("SELECT scope FROM "+dataset+" WHERE name = '"+file+"'").fetchall()[0][0]
        replicas_location = rucio_database.execute("SELECT has_replicas,location FROM "+dataset+" WHERE name = '"+file+"' and rse='"+rse+"'").fetchall()
        replicas=replicas_location[0][0]
        location=replicas_location[0][1]
        if replicas !=0:
            #find a copy of the file
            files_not_this_rse = rucio_database.execute("SELECT scope,name,location,rse FROM "+dataset+" WHERE name = '"+file+"' and rse!='"+rse+"'").fetchall()
            print(files_not_this_rse)
        else:
            files_not_this_rse=[]
        database.execute('INSERT INTO "'+dataset+'" ("scope", "name", "location", "replicas") VALUES ("'+scope+'", "'+file+'","'+location+'", "'+str(files_not_this_rse)+'")')
    database.commit()



def main():
    #for subdirectory in RSE,not files, which is located in the same directory as this script
    for rse in os.listdir(os.getcwd()+"/RSE"):
        print(rse)
        #for output directory exiist in subdirectory
        if os.path.isdir(os.getcwd()+"/RSE/"+rse+"/output"):

            #move and create dark data databases
            missing_from_rucio_database,missing_from_storage_database=move_and_create_dark_data_databases(rse)

            #for file in output directory
            for file in os.listdir(os.getcwd()+"/RSE/"+rse+"/output"):
                #in each file, read the content
                with open(os.getcwd()+"/RSE/"+rse+"/output/"+file) as f:
                    file_content = f.readlines()


                #remove whitespace characters like `\n` at the end of each line
                file_content = [x.strip() for x in file_content]
                files=file_content[1:]
                directory_storage=file_content[0]



                #check if in table rse, there is a row with the same content as the first line of the file
                datasets_and_directories=directories_database.execute("SELECT datasets,exist_at_rses FROM "+rse+" WHERE directory = '"+directory_storage+"'").fetchall()
                dataset=datasets_and_directories[0][0]
                exist_at_rses=datasets_and_directories[0][1]
                
                #in the table rucio_database named the same as the dataset, get the file names
                files_in_rucio = rucio_database.execute("SELECT name FROM {} WHERE rse = '{}'".format(dataset, rse)).fetchall()
                files_in_rucio = [x[0] for x in files_in_rucio]


                #compare the file names in the file and the file names in the table
                files_missing_storage_but_exist_in_rucio = [x for x in files_in_rucio if x not in files]
                files_missing_rucio_but_exist_in_storage = [x for x in files if x not in files_in_rucio]
                print("         files missing in rucio but exist in storage: ",len(files_missing_rucio_but_exist_in_storage))
                print("         files missing in storage but exist in rucio: ",len(files_missing_storage_but_exist_in_rucio))


                print("\n\n")
                print("         dataset: ",dataset)
                for file in files_missing_rucio_but_exist_in_storage:
                    fill_database_missing_storage(rse,dataset,missing_from_storage_database,file)          
                

            

            
            
