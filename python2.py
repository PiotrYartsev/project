#find out when file was created
from rucio.client import Client
from rucio.common.exception import RucioException
from rucio.common.utils import generate_uuid

import sys
import os
import time
import sys

CLIENT = Client()

#print(CLIENT.ping())

#print(CLIENT.get_metadata(scope="mc20", name="mc_v9-8GeV-1e-inclusive_1000_t1588350689.root"))
os.system("cd; cd rucio-client-venv; source bin/activate; rucio whoami")
L=os.popen("rucio get-metadata mc20:mc_v9-8GeV-1e-inclusive_1000_t1588350689.root").read()
Q=os.popen("rucio get-metadata mc20:mc_v9-8GeV-1e-inclusive_192_t1588318337.root").read()
#print(Q)
L2=L.split("\n")
L2=[s.strip('\n') for s in L2]
L3=[s.split(":") for s in L2]

Q2=Q.split("\n")
Q2=[s.strip('\n') for s in Q2]
Q3=[s.split(":") for s in Q2]

superlist=[]
rangs=100
for i in range(rangs):
    if i<rangs/2:
        superlist.append(L3)
    else:
        superlist.append(Q3)

#print(superlist[0])
#print("\n \n \n")
#print(superlist[1])
#print(L3)

data=[]
for line in range(len(L3)-1):
    otherdata=[]
    data.append([L3[line][0],otherdata])
#print(data)
for i in superlist:
    print(i)
    break
    for line in range(len(L3)-1):
        
        if data[line][1]==[]:
            #print(L3[line][1])
            data[line][1].append([L3[line][1],1])
        else:
            for j in range(len(data[line][1])):
                if data[line][1][j][0]==L3[line][1]:
                    data[line][1][j][1]+=1
                    break
                if j==len(data[line][1])-1:
                    data[line][1].append([L3[line][1],1])
                    break
                else:
                    data[line][1].append([L3[line][1].replace(" ", ""),1])
print("\n \n \n")
for i in range(100):
    print(L3)
    break
    for line in range(len(L3)-1):
        
        if data[line][1]==[]:
            #print(L3[line][1])
            data[line][1].append([L3[line][1],1])
        else:
            for j in range(len(data[line][1])):
                if data[line][1][j][0]==L3[line][1]:
                    data[line][1][j][1]+=1
                    break
                if j==len(data[line][1])-1:
                    data[line][1].append([L3[line][1],1])
                    break
                else:
                    data[line][1].append([L3[line][1].replace(" ", ""),1])
print(data[line])
    
#print(data)