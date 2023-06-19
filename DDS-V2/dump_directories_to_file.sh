#!/bin/bash

# This script reads a file line by line, replaces a string in each line, lists the contents of the directory with the same name as each line, and saves the output to a file with the same name as each line in the "output" subdirectory of the directory where the script is run.

# Check that the first command-line argument is provided and is a valid file
if [ $# -eq 0 ]; then
    echo "Error: no input file specified"
    exit 1
elif [ ! -f "$1" ]; then
    echo "Error: $1 is not a valid file"
    exit 1
fi

# Read the replacement strings from the "where_am_i.txt" file in the script directory
script_dir="$(dirname "$0")"
where_am_i_file="$script_dir/where_am_i.txt"
if [ ! -f "$where_am_i_file" ]; then
    echo "Error: where_am_i.txt not found in script directory"
    exit 1
fi
IFS=',' read -r name where_am_i replace_with < "$where_am_i_file"

# Remove the first object from the second line
where_am_i=${where_am_i#*:}
echo $where_am_i


# Start the loop
while read line
do

    # Replace the string in the line with the specified replacement
    line=${line//$where_am_i/$replace_with}
    # Save the output to a file in the "output" subdirectory of the directory where the script is run
    echo "Listing directory: $line"
    output_file="$(basename "$line").txt"
    mkdir -p "$script_dir/output"
    ls "$line" > "$script_dir/output/$output_file"
    echo "Saved output to file: $script_dir/output/$output_file"
done < "$1"
