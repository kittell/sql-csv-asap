from utils import *
import time
import csv


def get_index_directory(table_name=''):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    working_directory = os.getcwd()
    index_directory = os.path.join(working_directory, 'index')

    # If index_directory doesn't exist, create it
    if os.path.isdir(index_directory) == False:
        os.mkdir(index_directory)
    
    # Make a dir one level down if table_name != ''
    if table_name != '':
        index_directory = os.path.join(index_directory, table_name)
        if os.path.isdir(index_directory) == False:
            os.mkdir(index_directory)
    
    return index_directory


def is_index_file(f):
    """IS_INDEX_FILE
        DESCRIPTION: Test whether target file is an index file
        INPUT: string f: fullpath of a file
        OUTPUT: boolean result: true if file is an index file; otherwise, false
    """
    result = False
    if 'index-' in f:
        result = True
    return result

def parse_index_filename(index_file):
    """PARSE_INDEX_FILENAME
        DESCRIPTION: An index filename contains information about which table-attribute
            it is indexing, as well as which type of index is being used. Extract this
            information when scanning index files.
        INPUT: string index_file: filename or fullpath of a file
        OUTPUT: list index_file_parts: [table_name, index_type, attr_name]
    """
    # Break it down.
    # The format is: TABLENAME__index-TYPE-ATTRIBUTE.txt
    index_file_parts = []
    
    # 1) Remove path, if necessary
    index_file = os.path.basename(index_file)

    # 2) Check if it's an index file
    if is_index_file(index_file) == False:
        return index_file_parts

    # 3) Strip off .txt
    index_file = index_file[:-len('.txt')]

    # 4) Then split on double-underscore. 
    #       table_name is in position 0 after split. index-TYPE-ATTRIBUTE in position 1.
    index_file_split1 = index_file.split('__')
    index_file_parts.append(index_file_split1[0])

    # 5) Split the second part on dash
    #       index_type is in position 1. attr_name is in position 2.
    index_file_split2 = index_file_split1[1].split('-')
    index_file_parts.append(index_file_split2[1])
    index_file_parts.append(index_file_split2[2])
    
    return index_file_parts
    
def get_query_index_list(table_list):
    """GET_QUERY_INDEX_LIST
        DESCRIPTION: Builds a dict to organize existing indexes by table.attr used in query
        INPUT: table_list: list of tables used in query, to limit size of resulting dict
        OUTPUT: dict of indexes for query. key: table.attr pair; value: list of relevant indexes
            e.g., index_dict[table.attr] = [index1.txt, index2.txt]
    """
    index_dict = {}
    for table_name in table_list:
        # Go to indexes/table_name. Determine which attributes have an index there.
        index_directory = get_index_directory(table_name)
        for item in os.listdir(index_directory):
            if is_index_file(item) == True:
                # If it's an index file: good.
                index_parts = parse_index_filename(item)
                ta = combine_table_attribute_pair(index_parts[0], index_parts[2])
        
                # Add table.attr as key to index_dict
                if ta not in index_dict:
                    index_dict[ta] = []
                
                # Add file to dict
                index_dict[ta].append(item)
            
    return index_dict

    
def get_index_list_table(table_name):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    # appends index fullpath to list for a single table
    index_list = []
    index_directory = get_index_directory(table_name)
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if is_index_file(f) == True:
            index_list.append(item)

    return index_list
    
def get_index_list_all():
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    # appends index filename (not path) to dict for all existing indexes
    index_dict = {}
    index_directory = get_index_directory()
    
    # 1) Walk through \indexes, gather directories as keys for index_dict
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if os.path.isdir(f) == True:
            index_dict[item] = get_index_list_table(item)
                
    return index_dict
    
def get_index_fullpath_keyword(table_name, keyword):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    index_directory = get_index_directory(table_name)
    index_components = [table_name, '__index-keyword-', keyword, '.txt']
    index_fullpath = os.path.join(index_directory, ''.join(index_components))
    return index_fullpath
    
    
def write_index_file_keyword(table_name, keyword, index_results_dict):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    index_fullpath = get_index_fullpath_keyword(table_name, keyword)
    
    # Just overwrite existing file if it already exists
    f = open(index_fullpath, 'w', encoding='utf-8')
    for k_sorted_tuple in sorted(index_results_dict.items()):
        # format of keyword index line:     k:pointer1,pointer2,p3,...
        k = k_sorted_tuple[0]
        writeline = str(k) + ':'
        for i in range(len(index_results_dict[k])):
            if i > 0:
                writeline = writeline + ','
            writeline = writeline + str(index_results_dict[k][i])
        writeline += '\n'
        f.write(writeline)
    f.close()
    return index_fullpath

def exists_index_file_keyword(table_name, keyword):
    """EXISTS_INDEX_FILE_KEYWORD
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    return os.path.exists(get_index_fullpath_keyword(table_name, keyword))

def read_index_file_keyword(table_name, keyword):
    """READ_INDEX_FILE_KEYWORD
        DESCRIPTION: For a given table_name and keyword (attribute), open the corresponding
            keyword index file. For each line, add the keyword into a dict as a key, and
            the following numbers on the line as the dict values. This represents an
            byte number index for each keyword in the table.
        INPUT: 
        OUTPUT: 
    """
    index_results_dict = {}
    index_fullpath = get_index_fullpath_keyword(table_name, keyword)
    
    f = open(index_fullpath, 'r')
    for line in f:
        # get rid of newline
        line = line.rstrip('\n')
        
        # split line over ':'
        # first part is dict key, second part is list of line pointers
        line_split = line.split(':')
        k = line_split[0]
        pointers = line_split[1].split(',')
        if line_split[0] not in index_results_dict:
            # it shouldn't already be in there, but might as well check...
            index_results_dict[k] = pointers
    
    f.close()
    
    return index_results_dict

def zero_pass_query(table_name, q):
    """ZERO_PASS_QUERY
        DESCRIPTION: See if all attributes in WHERE clause for a given table_name are
            covered by an index. If this is true, then a query can be performed without
            scanning an entire table, just by going to particular bytes in a file.
        INPUT: 
        OUTPUT: 
    """
    pass

"""INDEX COMMAND HANDLERS
"""

def cmd_index_show():
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    index_dict = get_index_list_all()
    
    # Handle empty case first
    if len(index_dict) == 0:
        print('\nNo indexes available\n')
    else:
        for table_name in index_dict:
            print('\n', table_name.upper(), ":")
            for index in index_dict[table_name]:
                print('  ', index)

def cmd_index_show_table(table_name):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    index_list = get_index_list_table(table_name)
    
    # Handle empty case first
    if len(index_list) == 0:
        print('\nNo indexes available\n')
    else:
        for index in index_list:
            print(index)
    
def cmd_index_create_keyword(table_name, keyword):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    # START TIMER - after receiving index command
    start_time = time.time()
    # TODO: might want to check against available tables so you don't create index for nonexistent table
    # TODO: need to protect against using old index file, maybe append unix time to filename
    
    # calling get_index_directory also creates directory if needed
    index_directory = get_index_directory(table_name)
    
    # Loop over table
    # keyword represents an attribute named in first row - get its index - k
    # keyword_index_dict[k] = [row1, row2, ..., row_n]
    # for every row (i_row), if v = row[k] is not blank:
    #   make sure v is a key in keyword_index_dict
    #   append i_row to keyword_index_dict[v]
    # When it's all over write to indexes/table_name/index_file_name
    
    csv_fullpath = table_to_csv_fullpath(table_name)
    attribute_list = get_attribute_list(csv_fullpath)
    attribute_dict = get_attribute_dict2([table_name])
    
    # get index # in attribute_list of keyword
    ta = combine_table_attribute_pair(table_name, keyword)
    k = get_attribute_index(ta, attribute_dict)
    
    # Create index in memory, write at the end
    index_results_dict = {}
    
    # b is the byte position in the file
    b = 0
    
    with open(csv_fullpath, 'rb') as f:
        while True:
            f.seek(b)
            (b_returned, line) = readline_like_csv(f)
            if not line:
                break
            # Assume the first row is the header. Skip it.
            # Also skip if it's a blank row.
            if b > 0 and line.strip() != '':
                for row in csv.reader([line]):
#                    print('row:',row)
#                    print('k:',k)
                    #TODO: fails here if attr doesn't exist in table
                    v = row[k]
    
                    # don't index null results
                    if v != '':
                        # add v as key in index_results_dict if necessary
                        if v not in index_results_dict:
                            index_results_dict[v] = []
        
                        index_results_dict[v].append(b)
    
            # After reading row, get the byte number of the cursor
            # b = f.tell()
            b = b_returned
                    
    # END TIMER - after indexing
    print("--- %s seconds ---" % (time.time() - start_time))
    # Write results to file
    print(write_index_file_keyword(table_name, keyword, index_results_dict))    
    return
    
def cmd_index_delete(table_name):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    index_directory = get_index_directory(table_name)
    index_list = get_index_list_table(table_name)
    
    for index_filename in index_list:
        index_fullpath = os.path.join(index_directory, index_filename)
        print('Deleting', index_filename)
        os.remove(index_fullpath)

def get_index_command():
    """GET_INDEX_COMMAND
        DESCRIPTION: Shows user options for index context, retrieves user input
        INPUT: None
        OUTPUT: string index_command: user command for index context
    """
    print()
    print('show                               - Show all existing indexes')
    print('show index TABLE                   - Show all indexes for table')
    print('create index TABLE keyword KEYWORD - Create a new keyword index')
    print('delete index TABLE                 - Deletes all indexes on table')
    print()
    
    index_command = input('index > ')
    # TODO: validate input
    
    return index_command
    
def index_command_handler():
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    user_index_command = get_index_command()
    
    if user_index_command != '':
        # cmd is the map from user input to program function
        cmd = {
            'show':cmd_index_show,
            'show index':cmd_index_show_table,
            'create index keyword':cmd_index_create_keyword,
            'delete index':cmd_index_delete
        }
        
        index_command_list = user_index_command.split(' ')
        
        # TODO - this is such a mess - do something better than this...
        # maybe it doesn't even need this kind of handler, but a simpler if structure
        if len(index_command_list) > 1:
            if index_command_list[0] == 'show' and index_command_list[1] == 'index':
                # get remainder of string after 'show index'
                index_command = 'show index'
                table_name = index_command[len('show index'):len(index_command)]
                table_name = table_name.strip()
                return cmd[index_command](table_name)
            elif index_command_list[0] == 'create' and index_command_list[1] == 'index':
                index_command = 'create index ' + index_command_list[3]
                return cmd[index_command](index_command_list[2], index_command_list[4])
            elif index_command_list[0] == 'delete' and index_command_list[1] == 'index':
                index_command = 'delete index'
                return cmd[index_command](index_command_list[2])
        else:
            return cmd[user_index_command]()