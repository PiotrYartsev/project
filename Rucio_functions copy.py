from rucio.client import Client

rucioclient = Client()

def list_replicas(rse_name, scope):
    dids_name = []
    files = rucioclient.list_replicas(dids=dids_name, rse_expression=rse_name)
    return files

files = list_replicas('LUND', 'mc20')
for file in files:
    print(file)