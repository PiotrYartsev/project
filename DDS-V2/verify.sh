#!/bin/bash

# Loop through all .txt files in the directory
for file in *.txt; do
    # Create a new file with the same name as the original file
    output_file="${file%.*}.txt"
    touch "$output_file"

    # Loop through each line in the file
    while read -r line; do
        # Find the location to the file
        file_location=$(echo "$line" | cut -d' ' -f1)

        # Check if the file is missing
        if [ ! -f "$file_location" ]; then
            # Write the file name to the output file
            echo "$line" >> "$output_file"
        fi
    done < "$file"
done