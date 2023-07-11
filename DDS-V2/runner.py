#this is the main software that runs all the other parts
#from Extract_from_rucio import extract_from_rucio
from archives_generator import archives_generator,move_to_archives

from generate_gridftp_instructional_files import adding_data_about_replicas

from generate_text_rse_docs import generate_txt

from read_rse_output_and_compare_to_rucio import find_dark_data

from dark_data_functions import transforming_to_database_from_txt

from verify import verify
import datetime
import shutil
import logging


# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler and set its level to INFO
file_handler = logging.FileHandler('myapp.log')
file_handler.setLevel(logging.INFO)

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


#Get the width of the terminal and print a welcome message
terminal_width, _ = shutil.get_terminal_size()
print('*' * terminal_width)
print('*' * terminal_width)
print('-' * terminal_width)
print("Welcome to the DDS v2 software, part of LDCS.")
print('-' * terminal_width)
print("This software creates a local copy of the LDCS Rucio database, perform some post processing and generate list of files at different RSEs.")
print("Another software, called the dumper, running on the RSE generates a dump of files at directories registered in Rucio for that RSE.")
print("This software compares the two lists and generates a list of files that are missing from Rucio or missing from storage.")
print("For files missing from storage, it finds all the replicas of that file at other locations, in case we want to recover the file.")
print("For files missing from Rucio it shecks if the file is a duplciate file")
print("These files are then verified to be lost by checking Rucio and the RSE again.")
print("The list of files that are lost are then sent to the LDCS team or deleted automaticaly.")
print('-' * terminal_width)
print("The printouts and error messages are saved in this log file for debugging purposes.")
print('-' * terminal_width)
print('*' * terminal_width)
print('*' * terminal_width)

#Get todays date, which is used for naming archives and finding the most resent storage dump
today=datetime.date.today()
today=str(today)
today=today.replace("-","_")

print("\n\n")
print("Generating a new archive directory for today's date")
archives_generator(today,logger)
"""
print("Extracting data from Rucio")
#extract_from_rucio()
"""
print("\n\n")
print("Generating instructional files for gridftp")
adding_data_about_replicas(logger)

print("\n\n")
print("Generating text files with information about files at what RSEs to be used by the GridFTP server")
generate_txt(logger)

print("\n\n")
print("Transforming the txt files to a database")
transforming_to_database_from_txt(today,logger)

print("\n\n")
print("Find all the dark data and categorize them")
find_dark_data(logger)

print("\n\n")
print("Verifying that the data missing from Rucio is actually missing and create a file for the RSE to verify the data missing from storage")
verify(logger)

print("\n\n")
print("Moving the log file to the archives directory")
move_to_archives(today,logger)
