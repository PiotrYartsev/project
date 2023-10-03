import pickle

filename = "datastructure.pkl"

# Open the file in read binary mode
with open(filename, "rb") as f:
    # Unpickle the data structure from the file
    data_structure = pickle.load(f)

# Access the rse_index attribute and print its keys
print(data_structure.dataset_index.keys())