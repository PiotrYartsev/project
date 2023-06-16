import signal
import sqlite3
import re
from tqdm import tqdm
from Rucio_functions import list_files_dataset, list_replicas


# Flag to indicate if the script should exit
should_exit = False


# Signal handler for interrupt signal
def handle_interrupt(signal, frame):
    global should_exit
    print("Interrupt signal received. Finishing current iteration...")
    should_exit = True


# Register the signal handler
signal.signal(signal.SIGINT, handle_interrupt)


def read_dataset_names_from_file(file_path):
    # Read a text file line by line to extract the dataset names
    with open(file_path) as f:
        lines = f.readlines()

    # Remove whitespace characters like `\n` at the end of each line
    dataset_names = [line.strip() for line in lines]

    return dataset_names


def extract_file_metadata(file_data):
    metadata = list_replicas(file_data["scope"], file_data["name"])[0]
    file_info = {
        "name": file_data["name"],
        "scope": file_data["scope"],
        "adler32": file_data["adler32"],
        "rse": [],
        "location": []
    }
    for rse, locations in metadata["rses"].items():
        file_info["rse"].append(rse)
        file_info["location"].append(locations[0])
    return file_info


def create_dataset_table(conn, table_name):
    conn.execute("""
        CREATE TABLE {} (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            scope TEXT NOT NULL,
            rse TEXT NOT NULL,
            adler32 TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            filenumber INTEGER NOT NULL,
            location TEXT NOT NULL
        );
    """.format(table_name))


def append_file_metadata_to_table(conn, table_name, file_info):
    data = []
    for i in range(len(file_info["name"])):
        name = file_info["name"][i]
        scope = file_info["scope"][i]
        adler32 = file_info["adler32"][i]
        rse = file_info["rse"][i]
        location = file_info["location"][i]
        namesplit = name.split("_")
        timestamp = namesplit[-1].replace(".root", "").replace("t", "")
        filenumber = namesplit[-2].replace("run", "")
        data.append((name, scope, rse, adler32, timestamp, filenumber, location))

    sql = """
        INSERT INTO {} (name, scope, rse, adler32, timestamp, filenumber, location)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """.format(table_name)
    with conn:
        conn.executemany(sql, data)


def main():
    # Read dataset names from file
    dataset_names = read_dataset_names_from_file("datasets_and_numbers.txt")

    # Connect to database
    conn = sqlite3.connect('Rucio_data_LUND_GRIDFTP.db')

    # Create dataset table if it does not already exist
    if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dataset';").fetchall():
        conn.execute("""
            CREATE TABLE dataset (
                name TEXT NOT NULL,
                length INTEGER NOT NULL
            );
        """)

    for dataset_name in dataset_names:
        if should_exit:
            print("Exiting due to interrupt signal...")
            break

        try:
            scope, name = dataset_name.split(":")
            table_name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
            if table_name[0].isdigit():
                table_name = "x" + table_name

            print("Processing dataset: {}".format(name))

            # Check if dataset is already in the table
            if conn.execute("SELECT name FROM dataset WHERE name='{}'".format(name)).fetchall():
                print("The dataset {} is already in the table".format(name))

                # Compare count to length of dataset in the table and the number of files in the dataset in Rucio
                number_in_table = conn.execute("SELECT length FROM dataset WHERE name='{}'".format(name)).fetchone()[0]
                output = list(list_files_dataset(scope, name))
                number_in_rucio = len(output)

                if number_in_table != number_in_rucio:
                    raise Exception("Error: number of files in table does not match number of files in Rucio")
            else:
                print("The dataset {} is not in the table".format(name))

                # Get file metadata from Rucio
                output = list(list_files_dataset(scope, name))
                file_info = {"name": [], "scope": [], "adler32": [], "rse": [], "location": []}
                for file_data in tqdm(output):
                    file_info = extract_file_metadata(file_data)
                length = len(file_info["name"])

                # Add dataset to dataset table
                conn.execute("INSERT INTO dataset (name, length) VALUES (?, ?)", (name, length))

                # Create dataset table and add file metadata to table
                create_dataset_table(conn, table_name)
                append_file_metadata_to_table(conn, table_name, file_info)
        except Exception as e:
            raise Exception("An error occurred: {}".format(str(e)))

    # Cleanup code or any final actions before exiting
    print("Exiting the script...")


if __name__ == "__main__":
    main()