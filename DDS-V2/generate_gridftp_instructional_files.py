import sqlite3 as sl
import os
import shutil
import logging

def adding_data_about_replicas(logger):
    # Get the width of the terminal
    terminal_width, _ = shutil.get_terminal_size()

    logger.info('-' * terminal_width)
    logger.info('-' * terminal_width)
    logger.info("Adding data about replicas")
    
    # Initialize the databases
    rucio_database = sl.connect('Rucio_data_LUND_GRIDFTP.db')
    logger.info("Connected to Rucio database")

    # Next, for each RSE, generate a list of directories and files that Rucio believes exist at that RSE
    logger.info("Adding data about replicas for each table/dataset")

    # For each RSE in the table
    for table in (rucio_database.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'datasets'")):
        logger.info("\n")
        logger.info("Adding data about replicas for table {}".format(table[0]))

        table = table[0]
        if table not in ["dataset","sqlite_sequence"]:
            #check if the table has already been updated
            check = rucio_database.execute("SELECT exist_at_rses FROM dataset WHERE table_name = ?", (table,)).fetchall()
            if check[0][0] != None:
                logger.info("This table has already been updated")
                continue
            else:
                logger.info("This table has not been updated")
                #get the data from the table
                logger.info("Getting data from table {}".format(table))
                data = rucio_database.execute("SELECT rse,location  FROM {}".format(table)).fetchall()
                logger.info("Extracting information about RSEs and locations")
                rses = [i[0] for i in data]
                rses =list(set(rses))
                #make a string out of the list of rses without the brackets
                rses = str(rses)[1:-1]

                #from the data location which is i[1] for i in data, i want to remove everything after the last /
                data = [(i[0], os.path.dirname(i[1]).rstrip("\\")) for i in data]
                data=list(set(data))
                #make a string out of the list of data without the brackets
                data = str(data)[1:-1]


                #add this to the table called dataset where the $table 
                rucio_database.execute("UPDATE dataset SET exist_at_rses = ?, directory = ? WHERE table_name = ?", (rses,data, table))
                rucio_database.commit()
                logger.info("Added data about replicas for table {}".format(table))

    logger.info("Added data about replicas for each table/dataset, closing database")
    rucio_database.close()
    logger.info("Done")