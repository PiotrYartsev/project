import os 

file=open('files_found_storage.txt','r')
lines=file.readlines()
file.close()

newlines=[]
for line in lines:
    line=line.strip()
    line=line.split(",")
    filename=line[0]
    directory=line[3]
    output=directory+"/"+filename
    output=output.replace("//","/")
    newlines.append(output)
import datetime
#year, month, day
new_document=open("LUND_GRIDFTP_"+str(datetime.date.today())+".txt","w")
for line in newlines:
    new_document.write(line+"\n")
new_document.close()

