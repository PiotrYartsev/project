#this is the main software that runs all the other parts
#from Extract_from_rucio import extract_from_rucio
from archives_generator import archives_generator,move_to_archives

from generate_gridftp_instructional_files import adding_data_about_replicas

from generate_text_rse_docs import generate_txt
import datetime
import os
import shutil
#create a directory in archives with todays date
terminal_width, _ = shutil.get_terminal_size()
print('*' * terminal_width)
print('-' * terminal_width)
print("Welcome to the DDS v2 software. This software will generate the necessary files for the DDS v2.")
print('-' * terminal_width)
print('*' * terminal_width)
print("\n\n")


today=datetime.date.today()
today=str(today)
today=today.replace("-","_")

print("\n\n")
archives_generator(today)

print("\n\n")
#extract_from_rucio()

print("\n\n")
adding_data_about_replicas()

print("\n\n")
generate_txt()





print("\n\n")
move_to_archives(today)


