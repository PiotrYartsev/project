class FileMetadata:
    def __init__(self, name, scope, adler32, rse, location, has_replicas):
        self.name = name
        self.scope = scope
        self.adler32 = adler32
        self.rse = rse
        self.location = location
        self.has_replicas = has_replicas

class CustomDataStructure:
    def __init__(self):
        # Initialize dictionaries for each metadata category
        self.name_index = {}
        self.scope_index = {}
        self.adler32_index = {}
        self.rse_index = {}
        self.location_index = {}
        self.has_replicas_index = {}

    def add_item(self, item):
        # Create a FileMetadata instance and add it to all relevant indexes
        metadata = FileMetadata(
            item["name"],
            item["scope"],
            item["adler32"],
            item["rse"],
            item["location"],
            item["has_replicas"]
        )
        self.name_index[item["name"]] = metadata
        self.scope_index[item["scope"]] = metadata
        self.adler32_index[item["adler32"]] = metadata
        self.rse_index[item["rse"]] = metadata
        self.location_index[item["location"]] = metadata
        self.has_replicas_index[item["has_replicas"]] = metadata

    def find_by_metadata(self, metadata_category, value):
        # Retrieve items by a specific metadata category and value
        if metadata_category == "name":
            return self.name_index.get(value)
        elif metadata_category == "scope":
            return self.scope_index.get(value)
        elif metadata_category == "adler32":
            return self.adler32_index.get(value)
        elif metadata_category == "rse":
            return self.rse_index.get(value)
        elif metadata_category == "location":
            return self.location_index.get(value)
        elif metadata_category == "has_replicas":
            return self.has_replicas_index.get(value)

# Create an instance of the custom data structure
data_structure = CustomDataStructure()

# Create instances of FileMetadata
metadata1 = FileMetadata(
    name="file1.txt",
    scope="scope1",
    adler32="12345",
    rse="rse1",
    location="/path1",
    has_replicas=1
)

metadata2 = FileMetadata(
    name="file2.txt",
    scope="scope2",
    adler32="67890",
    rse="rse2",
    location="/path2",
    has_replicas=0
)

# Add instances of FileMetadata to the custom data structure
data_structure.add_item({
    "name": metadata1.name,
    "scope": metadata1.scope,
    "adler32": metadata1.adler32,
    "rse": metadata1.rse,
    "location": metadata1.location,
    "has_replicas": metadata1.has_replicas
})

data_structure.add_item({
    "name": metadata2.name,
    "scope": metadata2.scope,
    "adler32": metadata2.adler32,
    "rse": metadata2.rse,
    "location": metadata2.location,
    "has_replicas": metadata2.has_replicas
})

# Retrieve items by metadata category and value
result = data_structure.find_by_metadata("name", "file1.txt")

# Print the retrieved item
if result:
    print(result.name)
    print(result.scope)
    print(result.adler32)
    print(result.rse)
    print(result.location)
    print(result.has_replicas)
else:
    print("Item not found.")
