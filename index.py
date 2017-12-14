from utils import *
import time
import csv


def get_index_directory(table_name=''):
    working_directory = os.getcwd()
    index_directory = os.path.join(working_directory, 'indexes')

    # If index_directory doesn't exist, create it
    if os.path.isdir(index_directory) == False:
        os.mkdir(index_directory)
    
    # Make a dir one level down if table_name != ''
    if table_name != '':
        index_directory = os.path.join(index_directory, table_name)
        if os.path.isdir(index_directory) == False:
            os.mkdir(index_directory)
    
    return index_directory


def get_index_command():
    print()
    print('show                               - Show all existing indexes')
    print('show index TABLE                   - Show all indexes for table')
    print('create index TABLE keyword KEYWORD - Create a new keyword index')
    print('delete index TABLE                 - Deletes all indexes on table')
    print()
    
    index_command = input('index > ')
    # TODO: validation of input?
    
    return index_command

def is_index_file(f):
    result = False
    if os.path.isfile(f) == True:
        if 'index-' in f:
            result = True
    
    return result

def get_table_attribute_from_index_filename(index_file):
    # Break it down.
    # The format is: TABLENAME__index-TYPE-ATTRIBUTE.txt
    
    # First: strip off .txt
    index_file = indexfile.rstrip('.txt')
    
    # Then split on double-underscore
    index_file_parts = index_file.split('__')
    
    table_name = index_file_parts[0]
    
    # Attribute name is the third part of index_file_parts[1]
    index_info = index_file_parts[1]
    index_info_parts = index_info.split('-')
    
    # The attribute being indexed is the last part of index_info_parts
    attr_name = index_info_parts[2]
    
    return table_name + '.' + attr_name
    
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
                ta = get_table_attribute_from_index_filename(item)
        
                # Add table.attr as key to index_dict
                if ta not in index_dict:
                    index_dict[ta] = []
                
                # Add file to dict
                index_dict[ta].append(item)
            
    return index_dict

def get_index_list_table_attr(ta):
    index_list_table_attr = []
    table_name = ta[0]
    attr_name = ta[1]
    index_directory = get_index_directory()
    index_list_table = get_index_list_table(table_name)
    for i in index_list_table:
        ta2 = get_table_attribute_from_index_filename(i)
        if ta == ta2:
            index_list_table_attr.append(i)
    
    return index_list_table_attr
    
def get_index_list_table(table_name):
    # appends index fullpath to list for a single table
    index_list = []
    index_directory = get_index_directory(table_name)
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if is_index_file(f) == True:
            index_list.append(item)

    return index_list
    
def get_index_list_all():
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
    index_directory = get_index_directory(table_name)
    index_components = [table_name, '__index-keyword-', keyword, '.txt']
    index_fullpath = os.path.join(index_directory, ''.join(index_components))
    return index_fullpath
    
    
def write_index_file_keyword(table_name, keyword, index_results_dict):
    index_fullpath = get_index_fullpath_keyword(table_name, keyword)
    
    # Just overwrite existing file
    f = open(index_fullpath, 'w')
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
    return os.path.exists(get_index_fullpath_keyword(table_name, keyword))

def read_index_file_keyword(table_name, keyword):
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
    
"""INDEX COMMAND HANDLERS
"""

def cmd_index_show():
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
    index_list = get_index_list_table(table_name)
    
    # Handle empty case first
    if len(index_list) == 0:
        print('\nNo indexes available\n')
    else:
        for index in index_list:
            print(index)
    
def cmd_index_create_keyword(table_name, keyword):
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
    ta = table_name + '.' + keyword
    k = get_attribute_index(ta, attribute_dict)
    
    # For now - create index in memory, write at the end
    index_results_dict = {}
    
    with open(csv_fullpath, newline = '', encoding = 'utf-8') as f:
        r = csv.reader(f)
        next(r)     # Skip the header row
        for row in r:
            
            # Skip over blank rows, it's a killer
            if ''.join(row).strip() == '':
                continue
            
            #TODO: fails here if attr doesn't exist in table
            v = row[k]
            
            # don't index null results
            if v != '':
                # add v as key in index_results_dict if necessary
                if v not in index_results_dict:
                    index_results_dict[v] = []
                
                index_results_dict[v].append(r.line_num)
                
    f.closed
    
    # END TIMER - after indexing
    print("--- %s seconds ---" % (time.time() - start_time))
    # Write results to file
    print(write_index_file_keyword(table_name, keyword, index_results_dict))    
    return
    
def cmd_index_delete(table_name):
    index_directory = get_index_directory(table_name)
    index_list = get_index_list_table(table_name)
    
    for index_filename in index_list:
        index_fullpath = os.path.join(index_directory, index_filename)
        print('Deleting', index_filename)
        os.remove(index_fullpath)
    
def index_command_handler():
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
                index_command = index_command_list[0] + ' ' + index_command_list[1] + ' ' + index_command_list[3]
                return cmd[index_command](index_command_list[2], index_command_list[4])
            elif index_command_list[0] == 'delete' and index_command_list[1] == 'index':
                index_command = index_command_list[0] + ' ' + index_command_list[1]
                return cmd[index_command](index_command_list[2])
        else:
            return cmd[user_index_command]()
