import os
import os.path
import csv
import codecs
import operator


# Set TESTPRINT to True if you want to see intermediate calculations
TESTPRINT = True

def test_print(caption, term):
    # Print selected terms while testing
    # TODO: make the lists and dicts look better when printing
    if TESTPRINT == True:
        try:
            print(caption, ' : ', term)
        except UnicodeEncodeError:
            term = term.encode('ascii', 'ignore')
            print(caption, ' : ', term)

def get_table_directory():
	working_directory = os.getcwd()
	return os.path.join(working_directory, 'tables')

def get_csv_fullpath(csv_filename):
    # TODO: protection for when a full path is sent to this function
    csv_fullpath = os.path.join(get_table_directory(), csv_filename)
    return csv_fullpath

def get_filtered_table_fullpath(table):
    table_directory = get_table_directory()
    filtered_filename = 'temp_filtered_' + table + '.csv'
    return os.path.join(table_directory, filtered_filename)

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
    csv_filename = str(table_name) + '.csv'
    return csv_filename

def table_to_csv_fullpath(table_name):
    csv_filename = table_to_csv(table_name)
    csv_fullpath = get_csv_fullpath(csv_filename)
    return csv_fullpath

def csv_fullpath_to_table(csv_fullpath):
    csv_filename = os.path.basename(csv_fullpath)
    table_name = csv_to_table(csv_filename)
    
    return table_name

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
    print('\n***RESULTS***')
    for row in result_list:
        print(row)


def sql_not_like(a, b):
    """SQL_NOT_LIKE
    DESCRIPTION: Clumsy way of handling NOT LIKE as a SQL operator
    INPUT: 
    OUTPUT: 
    """
    
    return not sql_like(a, b)

def sql_like(a, b):
    """SQL_LIKE
    DESCRIPTION: Convert a given comparison operator 
        Case 1: % wildcard at start of b
        Case 2: % wildcard at end of b
        Case 3: % wildcard at start and end of b
    INPUT: 
    OUTPUT: 
    """
    # Assumption: max of two % wildcards in b, one at start, one at end
    # TODO: throw error if more than two %, or % in middle of string
    # TODO: account for '_' matching of a single character
    
    # Split b into wildcards and pattern
    # The resulting list will have an empty string where the wildcard was
    b_split = b.split('%')
#    print('a:', a)
#    print('b_split:', b_split)
    result = False
    
    if a == b:
        # Case 0: it's the same string
#        print('Case 0')
        result = True
    
    elif len(b_split) == 2:
        if b_split[0] == '':
            # Case 1: wildcard at start of b
            result = a.endswith(b_split[1])
#            print('Case 1:', result)
            
        elif b_split[1] == '':
            # Case 2: wildcard at end of b
            result = a.startswith(b_split[0])
#            print('Case 2:', result)
            
    elif len(b_split) == 3:
        if b_split[0] == '' and b_split[2] == '':
            # Case 3: wildcard at start and end of b
            if b_split[1] in a:
                result = True
#            print('Case 3:', result)
    
    return result

def get_comparison_function(c):
    """GET_COMPARISON_FUNCTION
    DESCRIPTION: Convert a given comparison operator 
    INPUT: 
    OUTPUT: 
    """
    
    # inspiration: https://stackoverflow.com/a/1740759/752784
    # operator library: https://docs.python.org/3/library/operator.html
    return {
            '=': operator.eq,
            '<': operator.lt,
            '<=': operator.le,
            '<>': operator.ne,
            '>': operator.gt,
            '>=': operator.ge,
            'AND': operator.and_,
            'OR': operator.or_,
            'NOT': operator.not_,
            'LIKE': sql_like,
            'NOT LIKE': sql_not_like
        }[c]

        
def eval_binary_comparison(a, op, b):
    """FUNCTION_NAME
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    
    return get_comparison_function(op)(a, b)


def parse_table_attribute_pair(ta):
    """PARSE_TABLE_ATTRIBUTE_PAIR
    DESCRIPTION: splits a table.attr string into [table, attr]
    INPUT: 
    OUTPUT: 
    """
    
    # For a table.attr pair, split into [table, attr]
    # Assumption: zero or one dots
    if '.' in ta:
        result = ta.split('.')
    else:
        result = ['', ta]
    return result

def combine_table_attribute_pair(t, a):
    result = ''
    if t != '':
        result = t + '.'
    result = result + a
    return result

def get_attribute_index(ta, attribute_dict):
    if '.' in ta:
        # protection against different kind of input
        ta = parse_table_attribute_pair(ta)
    table = ta[0]
    attr = ta[1]
    
    result = None
    
    for i in range(len(attribute_dict[table])):
        if attr == attribute_dict[table][i]:
            result = i
            break
    
    return result

def remove_temp_files():
    file_start_strings = ['temp_']
    table_directory = get_table_directory()
    file_list = os.listdir(table_directory)
    for file in file_list:
        for pattern in file_start_strings:
            if os.path.basename(file).startswith(pattern) == True:
                test_print('remove_temp_files / file', file)
                os.remove(os.path.join(table_directory, file))
