import os
import datetime
import shutil

def archives_generator(today):
    # Get the width of the terminal

    terminal_width, _ = shutil.get_terminal_size()

    print('-' * terminal_width)
    print('-' * terminal_width)
    print("Generating a new archive directory for today's date")
    
    #create a directory in archives with todays date


    #if the directory with that name already exist, delete it and create a new one
    if os.path.exists("archives/"+today):
        print("The directory already exists. Deleting it and creating a new one")
        os.system("rm -r archives/"+today)

    os.system("mkdir archives/"+today)
    print("The directory has been created")
    print('-' * terminal_width)
    print('-' * terminal_width)


def move_to_archives(today):
    #move the log file to the archives directory
    os.system("mv log.txt archives/"+today+"/log.txt")