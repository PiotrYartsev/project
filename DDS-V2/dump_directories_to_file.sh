#!/bin/bash
# 

#this srcipt will read a file line by line, as ls the directory with the name of the line
#and dump the result to a file with the name of the line

#the file to read from
#2023-06-18_10-20-30.txt

#the directory to dump to
#2023-06-18_10-20-30

#start the loop
while read line
do
    #ls the directory
    ls $line > $line.txt
done < $1





