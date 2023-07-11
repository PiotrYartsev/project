import os
import datetime
import shutil

def archives_generator(today,logger):
    # Get the width of the terminal
    terminal_width, _ = shutil.get_terminal_size()

    logger.info('-' * terminal_width)
    logger.info('-' * terminal_width)
    logger.info("Generating a new archive directory for today's date")
    
    #create a directory in archives with todays date

    #if the directory with that name already exist, delete it and create a new one
    if os.path.exists("archives/"+today):
        logger.info("The directory already exists. Deleting it and creating a new one")
        os.system("rm -r archives/"+today)

    os.system("mkdir archives/"+today)
    logger.info("The directory has been created")
    logger.info('-' * terminal_width)
    logger.info('-' * terminal_width)


def move_to_archives(today,logger):
    #move the log file to the archives directory
    os.system("mv myapp.log archives/"+today+"/myapp.log")
    logger.info("The log file has been moved to the archives directory for today's date")