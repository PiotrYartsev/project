#import libraries
import os
import sys
import subprocess
import time
import datetime


#for subdirectory in RSE,not files, which is located in the same directory as this script
for subdirectory in os.listdir(os.getcwd()+"/RSE"):
    print(subdirectory)
    #for output directory exiist in subdirectory
    if os.path.isdir(os.getcwd()+"/RSE/"+subdirectory+"/output"):
        #for file in output directory
        for file in os.listdir(os.getcwd()+"/RSE/"+subdirectory+"/output"):
            print(file)