#from rucio.client import Client
#import rucio.client as rucio
#Client= Client()

#l=(Client.list_scopes())
#print(l)
#print(list(Client.list_datasets_per_rse(rse='LUND', limit=10)))
#print(list(Client.get_rse_usage('LUND')))
#print(list(Client.list_rse_attributes('LUND')))
#print(list(Client.list_rses()))
#for scope in l:
#    print(scope)
#    L=(Client.list_replicas([{'scope':'{}'.format(scope)}], rse_expression='LUND'))
#    print(L)

#Why not use python rse wrapper? its inconsistent so I have no idea what to write where becouse sometimes dids have to be scope=mc20 and sometimes {"scope":"mc20"}
import os
from os.path import exists



os.system("cd; cd rucio-client-venv; source bin/activate")







"""
def get_scopes():
   scopes=list(os.popen("rucio list-scopes"))
   return(scopes)


def get_datasets_scopes(scopes):
    #currently broken, have to figure out why
    datasets_scopes=list(os.popen("rucio list-dids --filter type=DATASET test:*"))
    
    return(datasets_scopes)

def get_datasets_rse(rse):
    #currently broken
    datasets_rse=list(os.popen("rucio list-datasets-rse {}".format(rse))
    return(datasets_rse)
"""
#current bypas for the problems 
datasets=[]
for n in range(5):
    datasets.append("mc20:v9-8GeV-1e-inclusive")


def files_from_datasets(datasets):
    L2=[]
    for dataset in datasets:
        L=((os.popen("rucio list-file-replicas {} | grep LUND".format(dataset)).read()).split('\n'))
        #print(L)
        for l in L:
            l2=l.split('|')
            L2.append(l2)
        break
    return(L2)

def count_the_files(directory):
    pass


def get_info_from_data_storage(rse):
    if rse=="LUND":
        










        
    else:
        print

L2=files_from_datasets(datasets)
def 
for value in range(len(L2)-1):
    address=(L2[value][5])
    address=address.replace("LUND: file://", "")

    #print(address)

    
    fille=address[address.rindex('/')+1:]
    address=address.replace(fille,"")
    #print(address)
    print(fille)
    print(exists(address))

    os.system("cd; cd {}; pwd; echo {}; test -e {} && echo True || echo False".format(address,address,fille))
    break