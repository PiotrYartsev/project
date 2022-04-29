import os
from zlib import adler32
from datetime import datetime


a=open("classifier/All_LUND_GRIDFTP_2022-04-27_14:30:24.981447/files_missing_rucio/currupted_duplicate.txt", "r")

lines=a.readlines()

print(len(lines))

lines=[line for line in lines if not line=="\n"]

print(len(lines))
