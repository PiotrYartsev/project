import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Dark Data Search toolkit, software developed for the search and analysis of dark data at LDCS/LDMX.', formatter_class=argparse.RawDescriptionHelpFormatter)

    scopes_all_datasets = parser.add_mutually_exclusive_group(required=True)
    scopes_all_datasets.add_argument('--scopes', dest='scopes', nargs='+', metavar='SCOPE', help='Comma separated list of scopes for which to perform the dark data search', type=str)
    scopes_all_datasets.add_argument('--datasets', dest='datasets', nargs='+', metavar='DATASET', help='Comma separated list of datasets for which to perform the dark data search', type=str)
    scopes_all_datasets.add_argument('--all', dest='all', action='store_true', help='Perform the dark data search for all datasets in the Rucio instance')

    #For which if the RSE (Rucio Storage Element) should the dark data search be performed. Only one is allowed
    parser.add_argument('--rse', dest='rse', action='store', help='RSE to use for the dark data search', required=True)

    #The worker threads are used to perform the dark data search in parallel, splitting the work between the threads evenly
    parser.add_argument('--treads', dest='treads', action='store', type=int, default=1, help='Number of threads to use for the dark data search')

    #Post comparison, when the dark data is already found, the following options can be used to perform a more detailed analysis
    parser.add_argument('--replica-search', dest='replica_search', action='store_true', help='Look for replicas to replace the dark data', default=False)
    parser.add_argument('--duplicate-search', dest='duplicate_search', action='store_true', help='Look if dark data is coused by duplciate files', default=False)

    args = parser.parse_args()

    # Split comma-separated scopes into a list
    if args.scopes:
        args.scopes = [scope.strip() for scope in args.scopes[0].split(',')]

    # Split comma-separated datasets into a list
    if args.datasets:
        args.datasets = [dataset.strip() for dataset in args.datasets[0].split(',')]

    return args