import os
from zlib import adler32
from datetime import datetime


a=open("output/All_LUND_2022-04-28_20:59:54.348727/files_found_storage.txt", "r")

lines=a.readlines()

print(len(lines))

lines=list(set(lines))

print(len(lines))
