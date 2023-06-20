#import libraries
import os
import sys
import subprocess
import time
import datetime


#for subdirectory in RSE,not files, which is located in the same directory as this script
for rse in os.listdir(os.getcwd()+"/RSE"):
    print(rse)
    #for output directory exiist in subdirectory
    if os.path.isdir(os.getcwd()+"/RSE/"+rse+"/output"):
        #for file in output directory
        for file in os.listdir(os.getcwd()+"/RSE/"+rse+"/output"):
            #in each file, read the first line
            with open(os.getcwd()+"/RSE/"+rse+"/output/"+file) as f:
                first_line = f.readline()
                print(first_line)