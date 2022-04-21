import os
from zlib import adler32
from datetime import datetime

from run_test import *

a=list(os.popen("ls /home/pioyar/alkfmnsl || echo false"))
print(a)
if ['false\n']==a:
    print(a)