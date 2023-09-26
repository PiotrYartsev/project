class FileMetadata:
    def __init__(self, name, dataset, scope, rse, adler32, timestamp, filenumber, location, has_replicas):
        self.name = name
        self.dataset = dataset
        self.scope = scope
        self.rse = rse
        self.adler32 = adler32
        self.timestamp = timestamp
        self.filenumber = filenumber
        self.location = location
        self.has_replicas = has_replicas

    def __getitem__(self, key):
        return getattr(self, key)
class CustomDataStructure:
    def __init__(self):
        # Initialize dictionaries (indexes) for each metadata category
        self.name_index = {}
        self.dataset_index = {}
        self.rse_index = {}
        self.scope_index = {}
        self.adler32_index = {}
        self.timestamp_index = {}
        self.filenumber_index = {}
        self.location_index = {}
        self.has_replicas_index = {}

    def add_item(self, item):
        # Create a FileMetadata instance
        metadata = FileMetadata(
            item["name"],
            item["dataset"],
            item["rse"],
            item["scope"],
            item["adler32"],
            item["timestamp"],
            item["filenumber"],
            item["location"],
            item["has_replicas"]
        )
        
        # Append the metadata to the relevant indexes
        self._append_to_index(self.name_index, item["name"], metadata)
        self._append_to_index(self.dataset_index, item["dataset"], metadata)
        self._append_to_index(self.rse_index, item["rse"], metadata)
        self._append_to_index(self.scope_index, item["scope"], metadata)
        self._append_to_index(self.adler32_index, item["adler32"], metadata)
        self._append_to_index(self.timestamp_index, item["timestamp"], metadata)
        self._append_to_index(self.filenumber_index, item["filenumber"], metadata)
        self._append_to_index(self.location_index, item["location"], metadata)
        self._append_to_index(self.has_replicas_index, item["has_replicas"], metadata)

    def find_by_metadata(self, metadata_category, value):
        # Retrieve items by a specific metadata category and value
        index = getattr(self, f"{metadata_category}_index")
        return index.get(value)

    def _append_to_index(self, index, key, value):
        # Helper method to append metadata to an index (dictionary) allowing for multiple values
        if key in index:
            index[key].append(value)
        else:
            index[key] = [value]

# Create a CustomDataStructure instance
custom_structure = CustomDataStructure()

# Create some FileMetadata instances
file1 = {
    "name": "file1.txt",
    "dataset": "data1",
    "rse": "rse1",
    "scope": "scope1",
    "adler32": "12345",
    "timestamp": "2023-09-25",
    "filenumber": 1,
    "location": "Sweden",
    "has_replicas": True
}

file2 = {
    "name": "file2.txt",
    "dataset": "data2",
    "rse": "rse1",
    "scope": "scope2",
    "adler32": "67890",
    "timestamp": "2023-09-26",
    "filenumber": 2,
    "location": "USA",
    "has_replicas": False
}

# Add these FileMetadata instances to the CustomDataStructure
custom_structure.add_item(file1)
custom_structure.add_item(file2)

# Now you can retrieve file metadata efficiently using the indexes
files_by_rse = custom_structure.find_by_metadata("rse", "rse1")

# files_by_rse will contain both "file1.txt" and "file2.txt" objects with the same "rse" value.
for file in files_by_rse:
    print(file.rse)
