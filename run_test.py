from test_func import *
from test2 import *
import sys
import test2
import os

if __name__ == '__main__':
    valid_rses=list_rse()
    valid_scopes=list_scopes()

    #defoult values
    tqmdis=False
    comments=False

    for argument in sys.argv:
        #argument 1:rse 
        if "rse=" in argument:
            argument=argument.replace("rse=","")
            if "All" in argument:
                rses=valid_rses
            if len(argument)==0:
                raise ValueError("rse= can not be empty")
            if argument in valid_rses:
                rses=argument
            else:
                raise ValueError("rse={} is not a valid rse. Choose from {}.".format(argument,valid_rses))
    
        if "scopes=" in argument:
            argument=argument.replace("scopes=","")
            #argument 2 scopes
            if "All" in argument:
                scopes=valid_scopes
            else:
                argument=(argument).split(",")
                for n in argument:
                    if n not in valid_scopes:
                        raise ValueError("At least one of provided scopes is not valid. Choose from {}.".format(valid_scopes))
                else:
                    scopes=argument

        if "output=" in argument:
            argument=argument.replace("scopes=","")
            if "True" in argument:
                test2.tqmdis=False
                test2.comments=True
            else: 
                test2.tqmdis=True
                test2.comments=False
    try:
        rses
    except:    
        raise ValueError("No rses provided")
    try:
        scopes
    except:    
        raise ValueError("No rses provided")
        
    rses=rses.split(",")

    All_datasets=get_all_datasets(scopes)

    datasets_rse=files_from_datasets(All_datasets,rses)

    datasets_rse=clean_up_datasets_rse(datasets_rse)
    with open('datasets.txt', 'w') as f:
        for n in datasets_rse:
            print("writing {} to datasets.txt".format(n))
            for k in datasets_rse[n]:
                k.append(n)
                k=str(k)
                f.write(k)                    
        f.close()
    #compere_checksum(datasets_rse)
"""
datasets_rse={}
with open('datasets.txt','r') as f:
    lines=f.readlines()
    for line in lines:
        list_all=line.split(",")
        [a.replace("\n","") for a in list_all[1:]]
        [a.replace("[","") for a in list_all[1:]]
        [a.replace("]","") for a in list_all[1:]]

        datasets_rse[list_all[0]]=list_all[1:]
        print(datasets_rse)
        break
"""