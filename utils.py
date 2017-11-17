import os
import csv
import codecs

def get_test_mode():
    # Set this to True to print out some info as the program runs
    return False

def get_table_directory():
	working_directory = os.getcwd()
	return os.path.join(working_directory, 'tables')

def get_csv_fullpath(csv_filename):
    csv_fullpath = os.path.join(get_table_directory(), csv_filename)
    return csv_fullpath

def get_attribute_list(csv_fullpath):
    """
    DESCRIPTION: For a given path of a .csv file, return the list of attributes, 
        which is assumed to be the values in the first row of the file
    INPUT: csv_fullpath: full path and filename for target .csv file
    OUTPUT: attribute_list: list containing names of attributes (strings)
    """
    with open(csv_fullpath, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        attribute_list = next(reader)
    return attribute_list

def get_attribute_dict(attribute_dict, csv_fullpath):
    new_attribute_list = get_attribute_list(csv_fullpath)
    new_table_name = csv_to_table(os.path.basename(csv_fullpath))
    
    #TODO: protect against adding attribute_list for same table name
    attribute_dict[new_table_name] = new_attribute_list
    
    return attribute_dict
	
def csv_to_table(csv_filename):
    """
    DESCRIPTION: Remove file extension from .csv files
    INPUT: csv_filename: filename (with extension)
    OUTPUT: table name (filename without extension)
    """
    if csv_filename.endswith('.csv'):
        table_name = csv_filename[0:len(csv_filename)-4]
    else:
        # If it's not a .csv, just throw the original back over the wall...
        table_name = csv_filename
    return table_name

def table_to_csv(table_name):
    """
    DESCRIPTION: Turn table name into filename, i.e., add .csv
    INPUT: table name (filename without extension)
    OUTPUT: filename (with extension)
    """
    csv_filename = table_name + '.csv'
    return csv_filename

def get_csv_list():
    """
    DESCRIPTION: Get the list of .csv files from the \tables directory. These
        are the tables that can be queried by the program.
    INPUT: none
    OUTPUT: csv_list: list of filenames only of .csv files in \tables
    """
    # method for getting list of files for a directory comes from:
    # https://stackoverflow.com/a/41447012/752784

    csv_list = [f for f in os.listdir(get_table_directory()) if f.endswith('.csv')]
    return csv_list

def display_query_result(result_list):
    print('***RESULTS***')
    for row in result_list:
        print(row)