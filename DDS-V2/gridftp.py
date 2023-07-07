import os
from globus_sdk import TransferClient

# Set up the transfer client
tc = TransferClient()

# Set up the GridFTP endpoint
endpoint_id = 'ldmx#LDCS'
endpoint_path = '/path/to/your/directory'

# Set up the local directory
local_path = '/path/to/your/local/directory'

# Create the local directory if it doesn't exist
if not os.path.exists(local_path):
    os.makedirs(local_path)

# Download the directory from the GridFTP endpoint
tc.download(endpoint_id, endpoint_path, local_path, recursive=True)