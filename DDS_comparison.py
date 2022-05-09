from test_func import *
from test2 import *
import sys
import test2
import os



#add search by dataset's

#add a timestamp to dump all the output files to for easier tracking and no overwrite
if __name__ == '__main__':
    valid_rses=list_rse()
    valid_scopes=list_scopes()

    #default values
    global tqmdis
    global comments
    global limit
    global checksum
    
    tqmdis=False
    comments=False
    limit=0
    checksum=True
    test2.datasets2=None
    for argument in sys.argv:
        #argument 1:rse 
        if "rse=" in argument:
            argument=argument.replace("rse=","")
            if "All" in argument:
                test2.rses=valid_rses
            elif len(argument)==0:
                raise ValueError("rse= can not be empty")
            elif argument in valid_rses:
                test2.rses=argument
            else:
                raise ValueError("rse={} is not a valid rse. Choose from {}.".format(argument,valid_rses))
    
        if "scopes=" in argument:
            argument=argument.replace("scopes=","")
            #argument 2 scopes
            if "All" in argument:
                test2.scopes=valid_scopes
            else:
                argument=(argument).split(",")
                for n in argument:
                    if n not in valid_scopes:
                        raise ValueError("At least one of provided scopes is not valid. Choose from {}.".format(valid_scopes))
                else:
                    test2.scopes=argument

        if "output=" in argument:
            argument=argument.replace("scopes=","")
            if "True" in argument:
                test2.tqmdis=False
                test2.comments=True
            elif "False" in argument: 
                test2.tqmdis=True
                test2.comments=False
            else:
                raise ValueError("{} is not a valid setting for the output. Choose between False and True.".format(argument))
        if "checksum=" in argument:
            argument=argument.replace("checksum=","")
            if "True" in argument:
                test2.checksum=True
            elif "False" in argument: 
                test2.checksum=False
            else:
                raise ValueError("{} is not a valid setting for adler32 checksum. Choose between False and True.".format(argument))
        if "limit=" in argument:
            argument=argument.replace("limit=","")
            if isinstance(argument,str)==True:
                test2.limit=int(argument)
            elif isinstance(argument,int)==True:
                test2.limit=argument
            else:
                raise ValueError("Limit has to be an integer".format())
        if "datasets=" in argument:
            argument=argument.replace("datasets=","")
            if isinstance(argument,str)==True:
                test2.datasets2=argument.split(",")
            else:
                raise ValueError("Limit has to be an integer".format())
    try:
        test2.rses
    except:    
        raise ValueError("No rses provided")
    try:
        test2.limit
    except:
        test2.limit=0
    
    if test2.datasets2==None:
        pass
    else:
        try:
            test2.scopes
            test2.datasets2
        except:
            try:
                test2.scopes
            except:
                try:
                    test2.datasets2
                except:
                    raise ValueError("You have to provide at least a scope or a dataset.")

        else:
            raise ValueError("Can not search by scope and by dataset similtaniosly, please use only one.")

    if not test2.datasets2==None:
        
        number_of_files_in_dataset={}
        rses=test2.rses.split(",")
        All_datasets=test2.datasets2
        
        datasets_rse, number_of_files_in_dataset=files_from_datasets(All_datasets,rses)
        
        datasets_rse=clean_up_datasets_rse(datasets_rse)

        compere_checksum(datasets_rse,number_of_files_in_dataset)
    else:
        if test2.scopes==valid_scopes and test2.limit==0:
            test2.All=True
        number_of_files_in_dataset={}
        rses=test2.rses.split(",")

        All_datasets=get_all_datasets(test2.scopes)
        
        datasets_rse, number_of_files_in_dataset=files_from_datasets(All_datasets,rses)
        
        datasets_rse=clean_up_datasets_rse(datasets_rse)

        compere_checksum(datasets_rse,number_of_files_in_dataset)
