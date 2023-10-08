import argparse
from Rucio_functions import RucioFunctions

def get_args():
    parser = argparse.ArgumentParser(description='Dark Data Search toolkit, software developed for the search and analysis of dark data at LDCS/LDMX.', formatter_class=argparse.RawDescriptionHelpFormatter)

    parser = argparse.ArgumentParser(description='Dark Data Search toolkit, software developed for the search and analysis of dark data at LDCS/LDMX.', formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--scopes', dest='scopes', nargs='+', metavar='SCOPE', help='Comma separated list of scopes for which to perform the dark data search', type=str)
    parser.add_argument('--datasets', dest='datasets', nargs='+', metavar='DATASET', help='Comma separated list of datasets for which to perform the dark data search', type=str)
    parser.add_argument('--all', dest='all', action='store_true', help='Perform the dark data search for all datasets in the Rucio instance')

    #For which if the RSE (Rucio Storage Element) should the dark data search be performed. Only one is allowed
    parser.add_argument('--rse', dest='rse', action='store', help='RSE to use for the dark data search', required=True)

    #The worker threads are used to perform the dark data search in parallel, splitting the work between the threads evenly
    parser.add_argument('--threads', dest='threads', action='store', type=int, default=1, help='Number of threads to use for the dark data search')

    parser.add_argument('--localdb', dest='localdb', action='store_true', help='Use the local copy of the Rucio database if available', default=False)

    #Post comparison, when the dark data is already found, the following options can be used to perform a more detailed analysis
    parser.add_argument('--replica-search', dest='replica_search', action='store_true', help='Look for replicas to replace the dark data', default=False)
    parser.add_argument('--duplicate-search', dest='duplicate_search', action='store_true', help='Look if dark data is coused by duplciate files', default=False)

    args = parser.parse_args()
    if args.scopes:
        args.scopes = args.scopes[0].split(",")
    
    if args.datasets:
        args.datasets = args.datasets[0].split(",")




    return args

#Function that retrives a list of datasets that should be used for the dark data search
def get_datasets_from_args(args):
    #If we select --all, we want this code to run for all the files in Rucio. THis can not be done with --scopes or --datasets, as we are specifiying what we want to search for while also saying we want to search for everything
    if args.all:
        #If we also specify --scopes or --datasets, we can not search for everything
        if args.scopes or args.datasets:
            print("Error: cannot specify scopes or datasets when using --all")
            exit(1)
        else:
            print("Loading all datasets\n")
            # Retrieve a list of all the scopes
            scopes_in_rucio = RucioFunctions.list_scopes()
            
            #Retrieve the datasets in the scopes
            datasets = []

            #for each scope we retrive the datasets and add them to the list
            for scope in scopes_in_rucio:
                #We do not want to search for test or validation datasets, as they are not relevant. They have also in teh past caused issues with the dark data search
                if "." in scope or "test" in scope or "validation" in scope:
                    continue
                else:
                    #We retrieve the datasets in the scope and add them to the list
                    output_list = RucioFunctions.list_dataset(scope=scope)
                    #we add the scope to the dataset name, as we need it later
                    output_list2 = [(scope, dataset,args) for dataset in output_list]
                    datasets.extend(output_list2)

    #If we do not select --all, we want to search for specific datasets
    #Here we search for all datasets that belong to a specific scope
    elif args.scopes and not args.datasets:
        # Retrieve a list of datasets for the specified scopes
        scopes_in_rucio = RucioFunctions.list_scopes()
        scopes_arg = args.scopes
        # Check if the scopes are valid
        for scope in scopes_arg:
            if scope not in scopes_in_rucio:
                print(f"Error: scope {scope} is not valid")
                print("Valid scopes are: " + str(scopes_in_rucio))
                exit(1)
        # Retrieve the datasets in the scopes
        datasets = []
        for scope in scopes_arg:
            #We retrieve the datasets in the scope and add them to the list
            output_list = RucioFunctions.list_dataset(scope=scope)
            #we add the scope to the dataset name, as we need it later
            output_list2 = [(scope, name) for name in output_list]
            datasets.extend(output_list2)
    #Here we search for specific datasets. We do somethign similar to the scopes, but we check if the argument we provided 
    elif args.datasets and not args.scopes:
        print("Loading datasets {}.\n".format(args.datasets))
        # Retrieve a list of all the datasets
        datasets_arg = args.datasets
        scopes_in_rucio = RucioFunctions.list_scopes()
        datasets = []
        for scope in scopes_in_rucio:
            #We retrieve the datasets in the scope and add them to the list
            output_list = RucioFunctions.list_dataset(scope=scope)
            #we add the scope to the dataset name, as we need it later. We only add the datasets that we specified in the argument
            output_list2 = [(scope, dataset) for dataset in output_list if dataset in datasets_arg]
            datasets.extend(output_list2)
        #Check if the datasets are valid
        for dataset in datasets_arg:
            if dataset not in [a[1] for a in datasets]:
                print(f"Error: dataset {dataset} is not valid")
                print("Valid datasets are: " + str([a[1] for a in datasets]))
                exit(1)
    else:
        print("Error: no scopes or datasets specified")
        exit(1)
    return datasets
    
    #Allow for the search of both scoep and dataset at the same time. Could be relevant if datasets share name in different scopes, but for now we ignore
    """
    elif args.scopes and args.datasets:
        print("Loading datasets {} for scopes {}.\n".format(args.datasets, args.scopes))
        # Retrieve a list of datasets for the specified scopes and datasets
        scopes_in_rucio = RucioFunctions.list_scopes()
        scopes_arg = args.scopes
        datasets_arg = args.datasets
        # Check if the scopes are valid
        for scope in scopes_arg:
            if scope not in scopes_in_rucio:
                print(f"Error: scope {scope} is not valid")
                exit(1)
        # Retrieve the datasets in the scopes and datasets
        datasets = []
        for scope in scopes_arg:
            output_list = RucioFunctions.list_dataset(scope=scope)
            output_list2 = [(scope, dataset) for dataset in output_list if dataset in datasets_arg]
            datasets.extend(output_list2)
        for dataset in datasets_arg:
            if dataset not in [a[1] for a in datasets]:
                print(f"Error: dataset {dataset} is not valid")
                exit(1)"""