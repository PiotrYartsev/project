#from rucio.client import Client
#import rucio.client as rucio

import os

from os.path import exists
#Client= Client()

os.system("cd; cd rucio-client-venv; source bin/activate")

datasets=[]
for n in range(1):
    datasets.append("mc20:v9-8GeV-1e-inclusive")

#l=(Client.list_scopes())
#print(l)
#print(list(Client.list_datasets_per_rse(rse='LUND', limit=10)))
#print(list(Client.get_rse_usage('LUND')))
#print(list(Client.list_rse_attributes('LUND')))

#print(list(Client.list_rses()))

"""
for scope in l:
    print(scope)
    L=(Client.list_replicas([{'scope':'{}'.format(scope)}], rse_expression='LUND'))
    print(L)
"""
L2=[]
print(datasets[0])
for dataset in datasets:
    L=((os.popen("rucio list-file-replicas {} | grep LUND".format(dataset)).read()).split('\n'))
    #print(L)
    for l in L:
        l2=l.split('|')
        L2.append(l2)
    break
for value in range(len(L2)-1):
    address=(L2[value][5])
    address=address.replace("LUND: file://", "")

    #print(address)

    
    fille=address[address.rindex('/')+1:]
    address=address.replace(fille,"")
    #print(address)
    #print(fille)
    #print(exists(address))

    #address="/projects/hep/fs7/scratch/pflorido/ldmx-pilot/pilotoutput/ldmx/mc-data/v9/8.0GeV/"


    #fille="mc_v9-8GeV-1e-target_photonuclear_6563_t1589182453.root"

    os.system("cd; cd {}; pwd; echo {}; test -e {} && echo True || echo False".format(address,address,fille))
    

def count_the_files(directory):
    pass
