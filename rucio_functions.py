from rucio.client import Client
from urllib.parse import quote_plus
import subprocess as subprocess

class RucioFunctions:
    rucioclient = Client()

    @classmethod
    def list_scopes(cls):
        #Run the Rucio API command "list_scopes", that list the available scopes

        #Attempt to run the command, if it fails, print the error and return None
        try:
            scopes = cls.rucioclient.list_scopes()
            return scopes
        except Exception as e:
            print(f"Error listing scopes: {e}")
            return None

    @classmethod
    def list_rses(cls):
        #Run the Rucio API command "list_rses", that list the available RSEs
        try:
            rses = cls.rucioclient.list_rses()
            return rses
        except Exception as e:
            print(f"Error listing RSEs: {e}")
            return None

    @classmethod
    def list_files_dataset(cls, scope, name):
        #Run the Rucio API command "list_files", that list the files in a dataset. We use the "long" option to get more information about the files
        try:
            files = cls.rucioclient.list_files(scope=scope, name=name, long=True)
            return files
        except Exception as e:
            print(f"Error listing files in dataset: {e}")
            return None

    @classmethod
    def get_rse_usage(cls, rse):
        #Run the Rucio API command "get_rse_usage", that list information about rse. This is usefull if we want to run a comparison of total number of files in storage vs the number of files registered for that storage element
        try:
            rse_info = cls.rucioclient.get_rse_usage(rse)
            return rse_info
        except Exception as e:
            print(f"Error getting RSE usage: {e}")
            return None

    @classmethod
    def get_metadata(cls, scope, name):
        #Run the Rucio API command "get_metadata", that list the metadata of a file. Could be used for further analysis of the files, curretnly not implemented NOTE: Maybe something to implement in the future
        try:
            metadata = cls.rucioclient.get_metadata(scope, name)
            return metadata
        except Exception as e:
            print(f"Error getting metadata: {e}")
            return None

    @classmethod
    def list_replicas(cls, scope, name):
        #For a given file, list all the replicas of that file. This is used as the regular way to retrive information about the files only says which rse it is located at, not the full path. This also contains some other good metadata about the file
        try:
            replicas = cls.rucioclient.list_replicas(dids=[{'scope': scope, 'name': name}])
            return replicas
        except Exception as e:
            print(f"Error listing replicas: {e}")
            return None

    @classmethod
    def list_replicas_batch(cls, file_list):
        #Same as list_replicas, but for a list of files. This is used to speed up the process of getting the information about the files, as the list_replicas function is quite slow
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
        #Count the number of files in a dataset, used for comparison with local database
        try:
            #Get the dataset info. There does not exist a function to count the number of files in a dataset, so we have to get the dataset info and there the "available_length" key contains the number of files
            dataset_info = cls.rucioclient.list_dataset_replicas(scope, dataset_name, deep=True)
            #We iterate over the dataset_info and sum the "available_length" key
            length = 0
            for each in dataset_info:
                length = length + each["available_length"]
            return length
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
        #We have to run the CLI command isntead of the API does not seam to have the list_dataset function, even thought it is documented
        command = f"rucio list-dids --filter type=DATASET {scope}:*"
        try:
            # Run the command and capture the output
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
            #We remove the first 3 lines and the last 2 lines of the output, as they contain the header and not actual data
            dataset = result.stdout.strip().split("\n")[3:-2]
            #We remove the "DATASET" and the scope from the output. This is also remenents of the foramting, and is not needed
            dataset = [x.strip().replace("DATASET", "").replace(scope+":", "").replace("|", "").replace(" ", "") for x in dataset]
            #We return the dataset as a list
            return dataset
        except Exception as e:
            print(f"Error listing datasets: {e}")
            return None
        
    @classmethod
    def list_dataset_replicas_bulk_CLI(cls, scope, name, rse):
        # Get the list of files in the dataset at the specified RSE
        command = f"rucio list-file-replicas --rses {rse} {scope}:{name}"

        # Run the command and capture the output
        output = subprocess.check_output(command, shell=True)
        output = output.decode("utf-8").strip().split("\n")[3:-2]
        output = [x.replace(" ", "")[1:-1].split("|") for x in output]
        output = [(x[0], x[1], x[2], x[3], x[4]) for x in output if x[3] == rse]
        return output