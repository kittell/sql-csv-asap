from utils import *
import time
import csv


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
    # TODO - just checking if it's a file or not for now
    if os.path.isfile(f) == True:
        result = True
    
    return result
    
def get_index_list_selected_tables(table_list):
    index_dict = {}
    for table_name in table_list:
        # Add table_name as key to index_dict
        if table_name not in index_dict:
            index_dict[table_name] = []
        
        # Add found index files to list
        index_directory = get_index_directory(table_name)
        index_dict[table_name] = get_index_list_table(table_name)
    
    return index_dict
    
def get_index_list_table(table_name):
    index_list = []
    index_directory = get_index_directory(table_name)
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if is_index_file(f) == True:
            # TODO - do some further pretty-processing of filenames so they make sense
            index_list.append(item)

    return index_list
    
def get_index_list_all():
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
    index_components = [table_name, '_index-keyword-', keyword, '.txt']
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
        i_row = 1
        r = csv.reader(f)
        next(r)     # Skip the header row
        for row in r:
            i_row = i_row + 1
            
            # Skip over blank rows, it's a killer
            if ''.join(row).strip() == '':
                continue
            
            v = row[k]
            
            # don't index null results
            if v != '':
                # add v as key in index_results_dict if necessary
                if v not in index_results_dict:
                    index_results_dict[v] = []
                
                index_results_dict[v].append(i_row)
                
    f.closed
    
    # Write results to file
    print(write_index_file_keyword(table_name, keyword, index_results_dict))
    
    read_index_file_keyword(table_name, keyword)
    
def cmd_index_delete(table_name):
    print('cmd_index_delete not implemented')
    
def index_command_handler():
    index_command = get_index_command()
    
    if index_command != '':
        # START TIMER - after receiving index command
        start_time = time.time()
    
        # cmd is the map from user input to program function
        cmd = {
            'show':cmd_index_show,
            'show index':cmd_index_show_table,
            'create index keyword':cmd_index_create_keyword,
            'delete index':cmd_index_delete
        }
        
        index_command_list = index_command.split(' ')
        
        if index_command_list[0] == 'show' and index_command_list[1] == 'index':
            # get remainder of string after 'show index'
            table_name = index_command[len('show index'):len(index_command)]
            table_name = table_name.strip()
            index_command = 'show index'
            return cmd[index_command](table_name)
        elif index_command_list[0] == 'create' and index_command_list[1] == 'index':
            index_command = index_command_list[0] + ' ' + index_command_list[1] + ' ' + index_command_list[3]
            # unncessary to pull these out, but want to make code more understandable
            table_name = index_command_list[2]
            index_type = index_command_list[4]
            return cmd[index_command](table_name, index_type)
        else:
            return cmd[index_command]()
        
        # TODO - obviously putting this after the returns is useless..........
        # END TIMER - after indexing
        print("--- %s seconds ---" % (time.time() - start_time))