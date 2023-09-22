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

    #Post comparison, when the dark data is already found, the following options can be used to perform a more detailed analysis
    parser.add_argument('--replica-search', dest='replica_search', action='store_true', help='Look for replicas to replace the dark data', default=False)
    parser.add_argument('--duplicate-search', dest='duplicate_search', action='store_true', help='Look if dark data is coused by duplciate files', default=False)

    args = parser.parse_args()

    # Split comma-separated scopes into a list
    if args.scopes:
        args.scopes = [scope.strip() for scope in args.scopes[0].split(',')]
        args.scopes = [i for i in args.scopes if "." in i or "test" in i or "validation" in i]

    # Split comma-separated datasets into a list
    if args.datasets:
        args.datasets = [dataset.strip() for dataset in args.datasets[0].split(',')]

    return args

def get_datasets_from_args(args):
    if args.all:
        if args.scopes or args.datasets:
            print("Error: cannot specify scopes or datasets when using --all")
            exit(1)
        else:
            print("Loading all datasets\n")
            # Retrieve a list of all the datasets
            scopes_in_rucio = RucioFunctions.list_scopes()
            datasets = []
            for scope in scopes_in_rucio:
                if "." in scope or "test" in scope or "validation" in scope:
                    continue
                else:
                    output_list = RucioFunctions.list_dataset(scope=scope)
                    output_list2 = [(scope, dataset) for dataset in output_list]
                    datasets.extend(output_list2)

    elif args.scopes and not args.datasets:
        print("Loading datasets for scopes {}.\n".format(args.scopes))
        # Retrieve a list of datasets for the specified scopes
        scopes_in_rucio = RucioFunctions.list_scopes()
        scopes_arg = args.scopes
        # Check if the scopes are valid
        for scope in scopes_arg:
            if scope not in scopes_in_rucio:
                print(f"Error: scope {scope} is not valid")
                exit(1)
        # Retrieve the datasets in the scopes
        datasets = []
        for scope in scopes_arg:
            output_list = RucioFunctions.list_dataset(scope=scope)
            output_list2 = [(scope, name) for name in output_list]
            datasets.extend(output_list2)

    elif args.datasets and not args.scopes:
        print("Loading datasets {}.\n".format(args.datasets))
        # Retrieve a list of all the datasets
        datasets_arg = args.datasets
        scopes_in_rucio = RucioFunctions.list_scopes()
        datasets = []
        for scope in scopes_in_rucio:
            output_list = RucioFunctions.list_dataset(scope=scope)
            output_list2 = [(scope, dataset) for dataset in output_list if dataset in datasets_arg]
            datasets.extend(output_list2)
        for dataset in datasets_arg:
            if dataset not in [a[1] for a in datasets]:
                print(f"Error: dataset {dataset} is not valid")
                exit(1)

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
                exit(1)

    else:
        print("Error: no scopes or datasets specified")
        exit(1)
    
    return datasets