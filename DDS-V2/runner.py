#this is the main software that runs all the other parts
from Rucio_functions import RucioFunctions

from database_from_rucio import RucioDataset
#from archives_generator import archives_generator,move_to_archives

#from generate_gridftp_instructional_files import adding_data_about_replicas

#from generate_text_rse_docs import generate_txt

#from read_rse_output_and_compare_to_rucio import find_dark_data

#from dark_data_functions import transforming_to_database_from_txt

from argument_loader import get_args, get_datasets_from_args

from verify import verify
import os
import sqlite3 as sl
"""

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


#Get todays date, which is used for naming archives and finding the most resent storage dump
today=datetime.date.today()
today=str(today)
today=today.replace("-","_")


archives_generator(today,logger)


adding_data_about_replicas(logger)




transforming_to_database_from_txt(today,logger)


find_dark_data(logger)


verify(logger)


move_to_archives(today,logger)
"""



if __name__ == "__main__":
    args = get_args()
    print(args)
    datasets = get_datasets_from_args(args)
    print(datasets[:3])
    
    
    for dataset in datasets:
        files_in_dataset=(RucioFunctions.list_files_dataset(dataset[0],dataset[1]))
        break
    print(list(files_in_dataset))

