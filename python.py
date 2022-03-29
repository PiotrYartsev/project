from rucio.client import Client
import rucio.client as rucio

import os

Client= Client()
print('sas')
os.system("cd; cd rucio-client-venv; source bin/activate")

datasets=[]
for n in range(10):
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

for l in L2:
    print(l)

