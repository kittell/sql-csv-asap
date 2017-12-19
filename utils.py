import os
import os.path
import csv
import codecs
import operator
import string

# Set TESTMODE to True if you want to see intermediate calculations
"""DEBUGGING METHODS
"""
TESTMODE = True

def get_testmode():
    return TESTMODE

def test_print(caption, term):
    # Print selected terms while testing
    # TODO: make the lists and dicts look better when printing
    if TESTMODE == True:
        try:
            print(caption, ' : ', term)
        except UnicodeEncodeError:
            term = term.encode('ascii', 'ignore')
            print(caption, ' : ', term)


"""DIRECTORY AND FILENAME METHODS
"""

def get_directory(name):
    working_directory = os.getcwd()
    dir = os.path.join(working_directory, name)
    
    # If directory doesn't exist, create it
    if os.path.isdir(dir) == False:
        os.mkdir(dir)
    
    return dir

def get_table_directory():
    return get_directory('tables')

def get_temp_directory():
    return get_directory('temp')

def get_csv_fullpath(csv_filename):
    # TODO: protection for when a full path is sent to this function
    csv_fullpath = os.path.join(get_table_directory(), csv_filename)
    return csv_fullpath

def get_filtered_table_fullpath(table):
    dir = get_temp_directory()
    filtered_filename = 'temp_filtered__' + table + '.csv'
    return os.path.join(dir, filtered_filename)

def get_temp_join_fullpath(table1, table2):
    dir = get_temp_directory()
    join_filename = 'temp_join__' + table1 + '__' + table2 + '.csv'
    return os.path.join(dir, join_filename)
    

def get_attribute_list(csv_fullpath):
    """
    DESCRIPTION: For a given path of a .csv file, return the list of attributes, 
        which is assumed to be the values in the first row of the file
    INPUT: csv_fullpath: full path and filename for target .csv file
    OUTPUT: attribute_list: list containing names of attributes (strings)
    """
    # First: protect against receiving table_name instead of csv_fullpath...
    if '.csv' not in csv_fullpath:
        # Just assume it was a table_name; if not, better luck next time
        csv_fullpath = table_to_csv_fullpath(csv_fullpath)
    
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
	
def get_attribute_dict2(table_list):
    # Loop through table list
    # Build attribute_list for each table
    attribute_dict = {}
    for table_name in table_list:
        csv_fullpath = table_to_csv_fullpath(table_name)
        attribute_dict[table_name] = get_attribute_list(csv_fullpath)
    
    # Return attribute_list for each table
    
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
    """GET_CSV_LIST
    DESCRIPTION: Get the list of .csv files from the \tables directory. These
        are the tables that can be queried by the program.
    INPUT: none
    OUTPUT: csv_list: list of filenames only of .csv files in \tables
    """
    # method for getting list of files for a directory comes from:
    # https://stackoverflow.com/a/41447012/752784

    csv_list = [f for f in os.listdir(get_table_directory()) if f.endswith('.csv')]
    return csv_list
    
def get_table_list():
    """GET_TABLE_LIST
    DESCRIPTION: Like get_csv_list but return table_names instead of filenames
    INPUT: none
    OUTPUT: table_list: list of filenames only of .csv files in \tables
    """
    # method for getting list of files for a directory comes from:
    # https://stackoverflow.com/a/41447012/752784
    
    table_list = []
    csv_list = get_csv_list()
    for c in csv_list:
        # Remove last four char, i.e., .csv
        table_list.append(c[:-4])
    return table_list

def get_query_table_list(raw_query):
    """GET_QUERY_TABLE_LIST
    DESCRIPTION: Get a list of tables called out in a query. Don't confuse with building
        a list of tables available to query (i.e., in /tables folder)
    INPUT: raw_query string input from user
    OUTPUT: query_table_list: list of tables called out in query
    DEPENDENCY: string.punctuation
    """
    query_table_list = []
    full_table_list = get_table_list()

    # Remove punctuation from string
    # https://stackoverflow.com/a/34294398/752784
    # remove_this is string.punctuation, minus the -
    remove_this = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
    translator = str.maketrans('', '', remove_this)
    raw_query = raw_query.translate(translator)

    # Break raw query up so it's just a list of terms        
    broken_query = raw_query.split(' ')
    
    # Loop over broken_query, finding terms that match full_table_list
    for term in broken_query:
        for table_name in full_table_list:
            if term == table_name:
                query_table_list.append(table_name)
                break
    
    return query_table_list


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
    # TODO: regex this
    
    # Split b into wildcards and pattern
    # The resulting list will have an empty string where the wildcard was
    b_split = b.split('%')
#    print('a:', a)
#    print('b_split:', b_split)
    result = False
    
    # Convert inputs into strings
    a = str(a)
    b = str(b)
    
    if a == b:
        # Case 0: it's the same string
        result = True
    
    elif len(b_split) == 2:
        if b_split[0] == '':
            # Case 1: wildcard at start of b
            result = a.endswith(b_split[1])
            
        elif b_split[1] == '':
            # Case 2: wildcard at end of b
            result = a.startswith(b_split[0])
            
    elif len(b_split) == 3:
        if b_split[0] == '' and b_split[2] == '':
            if b_split[1] in a:
                result = True
    
    return result

        
def eval_binary_comparison(a, op, b):
    """FUNCTION_NAME
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    # inspiration: https://stackoverflow.com/a/1740759/752784
    # operator library: https://docs.python.org/3/library/operator.html
    
    ops = {
        '=': operator.eq,
        '<>': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
        'AND': operator.and_,
        'OR': operator.or_,
        'NOT': operator.not_,
        'LIKE': sql_like,
        'NOT LIKE': sql_not_like
    }
    
    # Convert a and b to numbers, if possible
    # TODO: why doesn't this work with float(a)?
    try:
        a = int(a)
        b = int(b)
    except:
        pass
    
    result = ops[op](a, b)
    return result


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
        # No dots - assume it's just an attribute name, no table name
        result = ['', ta]
    return result

def combine_table_attribute_pair(t, a):
    result = ''
    if t != '':
        result = t + '.'
    result = result + a
    return result

def get_attribute_index(ta, attribute_dict):
    # Default input: table_attr_split [t, a]
    
    if '.' in ta:
        # Case: ta = table.attribute pair - split it out
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
    # Cleanup: remove files from /temp folder
    dir = get_temp_directory()
    file_list = os.listdir(dir)
    for file in file_list:
        if os.path.isfile(os.path.join(dir, file)) == True:
#            test_print('remove_temp_files / file', file)
            os.remove(os.path.join(dir, file))


def readline_like_csv(f):
    """READLINE_LIKE_CSV
    DESCRIPTION: The __next__ method in csv.reader disables f.tell, which is being used
        in indexing functions to get and write the byte position. The byte position can
        be found via f.tell when opening a file in 'rb' mode. However, when doing a 
        .readline() from a file, it doesn't respect that a newline \n inside double
        quotes should be part of the same field the way the csv module does. So: This 
        function tries to merge readlines when there is an open double-quote.
    INPUT: File f
    OUTPUT:
        - int f.tell(): allow calling code to skip any merged lines and not read again
        - string line: for most cases, just the readline retrieved from file; but if
        there is a double-quote broken across a newline, merges lines until both
        double-quotes are in the string
    NOTES: This was a real pain to balance using csv.reader and byte position together
        was not compatible...
    """
    # For the specific implementation, need to decode from byte mode.
    
    n_quotes = 0        # running count to determine open-close of double-quotes
    open_quotes = False
    prev_line = ''
    b = f.tell()
    
    while open_quotes == False:
        # TODO: debug this try-except block -- not sure if it's universal
        try:
            # Line is a byte object
            this_line = f.readline().decode(encoding='utf-8')
        except AttributeError:
            # Line has already been decoded into string
            f.seek(b)
            this_line = f.readline()
        except TypeError:
            f.seek(b)
            this_line = f.readline()

        line = prev_line + this_line
        n_quotes = line.count('"')
        
        if n_quotes %2 == 0:
            open_quotes = True
        prev_line = line
        
        b = f.tell()
                
    return b, line