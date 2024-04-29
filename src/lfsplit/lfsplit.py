import csv
import time

import dask.array as da
import dask.bag as db
import dask.dataframe as dd
import numpy as np


# Prints all ddf partitions
def print_partitions(ddf):
    for i in range(ddf.npartitions):
        print('')
        print(ddf.partitions[i].compute())

# Splits the input file
def split_file(args):
    start = time.time()
    argsdict = vars(args)

    # Read files and create ddf
    ddf1 = dd.read_csv('../' + argsdict['src_file_path'] + argsdict['src_file_name'], quoting = csv.QUOTE_NONE, quotechar = '', sep = argsdict['col_separator'], header = None, dtype = str, encoding_errors = 'ignore')
    print(ddf1.head())

    print('DataFrame created:', (time.time() - start), 'sec.')

    # Get the number of columns in the ddf and generate column names
    num_columns = len(ddf1.columns)
    column_names = ['col' + str(i) for i in range(num_columns)]
    ddf1.columns = column_names
    
    print('Header created:', (time.time() - start), 'sec.')

    # Format the candidate index column as "date" in a sortable format
    ddf1['idx_col'] = dd.to_datetime(ddf1[argsdict['partition_col']], format=argsdict['src_date_format'])
    
    print('Partition column formatted:', (time.time() - start), 'sec.')

    # Calculate and sort the divisions, each partition corresponds to a year-month
    years = ddf1['idx_col'].dt.year.unique().compute()
    divisions = [f"{year}-{month:02d}-01" for year in years for month in range(1, 13)]
    divisions.append(str(years.max() + 1) + '-01-01')
    divisions = sorted(divisions)
    divisions = [np.datetime64(x) for x in divisions]
    
    print('List divisions:')
    print(divisions)
    
    print('Divisions created:', (time.time() - start))

    # Partition the ddf
    ddf1 = ddf1.set_index('idx_col', divisions=divisions)
    
    print('Dataframe partitioned:', (time.time() - start))

    # Write the new partitioned ddf to file
    ddf1.to_csv('../' + argsdict['out_file_path'] + argsdict['out_file_name'], quoting = csv.QUOTE_NONE, quotechar = '', sep = argsdict['col_separator'], header = None, index = False)

    print('Partitioned files saved:' , (time.time() - start))

# Test args
def test_parameters(args):
    print(args)
