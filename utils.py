import os
import csv

def get_table_directory():
	working_directory = os.getcwd()
	return working_directory + '\\tables'


def get_attribute_list(csv_fullpath):
    with open(csv_fullpath, newline='') as f:
        reader = csv.reader(f)
        attribute_list = next(reader)
    return attribute_list
	
def csv_to_table(csv_filename):
    # csv_filename is the table name with the .csv extension
    # For now, assuming that all entries end in .csv, and removing last four char
    # probably should be a validation test just in case
    table_name = csv_filename[0:len(csv_filename)-4]
    return table_name

def table_to_csv(table_name):
    csv_filename = table_name + '.csv'
    return csv_filename

def get_csv_list():
    # I don't think this is the best way to do this, but it works for now...
    # os.walk through /tables directory and add files to filenames list,
    # then walk back through filenames list and throw out everything that isn't a .csv
    # TODO: Probably it's best to not add a non-csv to the list in the first place...
    
    # https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory#3207973
    f = []
    for (dirpath, dirnames, filenames) in os.walk(get_table_directory()):
        f.extend(filenames)
        break

    for entry in filenames:
        if '.csv' not in entry:
            filenames.remove(entry)

    return filenames
