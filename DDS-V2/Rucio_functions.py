from rucio.client import Client
from urllib.parse import quote_plus
import subprocess as subprocess
import os 

class RucioFunctions:
    rucioclient = Client()

    @classmethod
    def list_scopes(cls):
        try:
            scopes = cls.rucioclient.list_scopes()
            return scopes
        except Exception as e:
            print(f"Error listing scopes: {e}")
            return None

    @classmethod
    def list_rses(cls):
        try:
            rses = cls.rucioclient.list_rses()
            return rses
        except Exception as e:
            print(f"Error listing RSEs: {e}")
            return None

    @classmethod
    def list_files_dataset(cls, scope, name):
        try:
            files = cls.rucioclient.list_files(scope=scope, name=name, long=True)
            return files
        except Exception as e:
            print(f"Error listing files in dataset: {e}")
            return None

    @classmethod
    def get_rse_usage(cls, rse):
        try:
            rse_info = cls.rucioclient.get_rse_usage(rse)
            return rse_info
        except Exception as e:
            print(f"Error getting RSE usage: {e}")
            return None

    @classmethod
    def get_metadata(cls, scope, name):
        try:
            metadata = cls.rucioclient.get_metadata(scope, name)
            return metadata
        except Exception as e:
            print(f"Error getting metadata: {e}")
            return None

    @classmethod
    def list_replicas(cls, scope, name):
        try:
            replicas = cls.rucioclient.list_replicas(dids=[{'scope': scope, 'name': name}])
            return replicas
        except Exception as e:
            print(f"Error listing replicas: {e}")
            return None

    @classmethod
    def list_replicas_batch(cls, file_list):
        # Create a list of DIDs from the input file list
        dids = [{'scope': scope, 'name': name} for scope, name in file_list]
        try:
            # Use the list_replicas method to check for the existence of the files
            replicas = cls.rucioclient.list_replicas(dids=dids)
            replicas = list(replicas)
            return replicas
        except Exception as e:
            print(f"Error listing replicas in bulk: {e}")
            return None

    @classmethod
    def count_files_func(cls, scope, dataset_name):
        try:
            dataset_info = cls.rucioclient.list_dataset_replicas(scope, dataset_name, deep=True)
            length = 0
            dataset_info = list(dataset_info)
            for each in dataset_info:
                length = length + each["available_length"]
            return length, dataset_info
        except Exception as e:
            print(f"Error counting files in dataset: {e}")
            return None

    @classmethod
    def check_files_exist(cls, file_list):
        # Create a list of DIDs from the input file list
        dids = [{'scope': scope, 'name': name} for scope, name in file_list]

        try:
            # Use the list_replicas method to check for the existence of the files
            replicas = cls.rucioclient.list_replicas(dids=dids)
            results = []
            replicas = list(replicas)

            # Iterate over the list of replicas and append the (scope, name) tuples to the results list
            for replica in replicas:
                scope = replica['scope']
                name = replica['name']
                results.append((scope, name))

            return results
        except Exception as e:
            print(f"Error checking file existence: {e}")
            return None

    @classmethod
    def list_dataset(cls, scope):
        # Run the CLI command "rucio list-dids --filter type=DATASET scope:*"
        command = f"rucio list-dids --filter type=DATASET {scope}:*"
        try:
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
            dataset = result.stdout.strip().split("\n")[3:-2]
            dataset = [x.strip().replace("DATASET", "").replace(scope+":", "").replace("|", "").replace(" ", "") for x in dataset]
            return dataset
        except Exception as e:
            print(f"Error listing datasets: {e}")
            return None