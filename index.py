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
    # If the index_file comes in as a fullpath, remove the path from the basename
    index_file = os.path.basename(index_file)
    
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
    table_name = index_file_split1[0]

    # 5) Split the second part on dash
    #       index_type is in position 1. attr_name is in position 2.
    index_file_split2 = index_file_split1[1].split('-')
    index_type = index_file_split2[1]
    attr_name = index_file_split2[2]
    
    return (table_name, index_type, attr_name)
    
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

def get_index_list_table2(table_name):
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    # appends index fullpath to list for a single table
    index_dict = {}
    index_directory = get_index_directory(table_name)
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if is_index_file(f) == True:
            index_file_parts = parse_index_filename(f)
            attr_name = index_file_parts[2]
            if attr_name not in index_dict:
                index_dict[attr_name] = []
            index_dict[attr_name].append(item)
    return index_dict
    
def get_index_list_all2():
    """
        DESCRIPTION: 
        INPUT: 
        OUTPUT: index_dict_all[table_name][attr_name]
    """
    # appends index filename (not path) to dict for all existing indexes
    index_dict_all = {}
    index_directory = get_index_directory()
    
    # 1) Walk through \indexes, gather directories as keys for index_dict_all
    for item in os.listdir(index_directory):
        f = os.path.join(index_directory, item)
        if os.path.isdir(f) == True:
            index_dict_all[item] = {}

    # For each table_name, get the index files in that directory
    
    # TODO: Would be better to not add blank entries to dict. In the meantime, delete at end.
    delete_list = []
    
    for table_name in index_dict_all:
        table_index_directory = os.path.join(index_directory, table_name)
        for item in os.listdir(table_index_directory):
            f = os.path.join(table_index_directory, item)
            if is_index_file(f) == True:
                index_dict = get_index_list_table2(table_name)
                if len(index_dict) > 0:
                    index_dict_all[table_name] = index_dict
        if len(index_dict_all[table_name]) == 0:
            delete_list.append(table_name)
        
    # Remove empty entries
    for d in delete_list:
        del index_dict_all[d]
                    
    return index_dict_all

def get_index_fullpath(table_name, attr_name, type, extra=''):
    """
        DESCRIPTION: 
        INPUT: 
            - boolean map: return a map filename if True
        OUTPUT: string index_fullpath: combined filepath and filename of index file
    """
    index_directory = get_index_directory(table_name)
    filebase = table_name + '__index-' + type + '-' + attr_name
    
    # Add extra strings, like -map
    if extra != '':
        filebase = filebase + '-' + extra
    filename = filebase + '.txt'
    index_fullpath = os.path.join(index_directory, filename)
    return index_fullpath
    

def write_index_map_keyword(table_name, keyword, index_fullpath):
    """WRITE_INDEX_MAP_KEYWORD
        DESCRIPTION: Write an index map for a given keyword. An index map consists of, for
            each row, a keyword value, then separated by a :, a byte pointer to where
            that keyword value is located in the index file.
        INPUT: 
        OUTPUT: 
    """
    # TODO: Currently a simple 1:1 keyword:keyword map. Could be improved. Should be improved.
    
    # Build the map dict: key: attribute value (keyword); value: start byte position in index
    f = open(index_fullpath, 'rb')
    b = 0
    index_map = {}
    for line in f:
        line = line.decode(sys.stdout.encoding)
        (k, pointers) = get_index_keyword_key_value(line)
                
        if k not in index_map:
            index_map[k] = b
        b = f.tell()
    f.close()
    
    # Write the map
    
    map_fullpath = get_index_fullpath(table_name, keyword, 'keyword', 'map')
    
    f = open(map_fullpath, 'w')
    i = 0
    for k_sorted_tuple in sorted(index_map.items()):
        k = k_sorted_tuple[0]
        writeline = ''
        if i > 0:
            writeline = '\n'
        writeline += k + ':' + str(index_map[k])
        f.write(writeline)
        i += 1
    f.close()
    
    return map_fullpath
    
def get_index_map_keyword(table_name, keyword):
    # Return index_map dict from file
    
    map_fullpath = get_index_fullpath(table_name, keyword, 'keyword', 'map')
    
    index_map = {}
    f = open(map_fullpath, 'r')
    while True:
        line = f.readline()
        if not line:
            # No more lines in file
            break
            
        (k, pointers) = get_index_keyword_key_value(line)
        #Should only be one value returned in pointers list
        index_map[k] = int(pointers[0])
    f.close()
    
    return index_map
    
def get_pointer_from_map_keyword(table_name, keyword, index_map, value):
    # For a particular value, get the pointer to the line in the index file
    
    map_fullpath = get_index_fullpath(table_name, keyword, 'keyword', 'map')
    
    result = None
    if value in index_map:
        result = index_map[value]
    return result

def get_pointers_from_index_keyword(table_name, keyword, index_map, attr_value):
    # For a particular keyword value (attr_value), use the map to get the pointers
    # from the index.
    pointers = []
    
    index_fullpath = get_index_fullpath(table_name, keyword, 'keyword')
    
    b = get_pointer_from_map_keyword(table_name, keyword, index_map, attr_value)
    
    f = open(index_fullpath, 'rb')
    f.seek(b)
    line = f.readline()
    line = line.decode(sys.stdout.encoding)
    f.close()
    
    (k, pointers) = get_index_keyword_key_value(line)
        
    return pointers

    
def write_index_file_keyword(table_name, keyword, index_results_dict):
    """WRITE_INDEX_FILE_KEYWORD
        DESCRIPTION: Write an keyword index file. On each row, a keyword value, then 
            separated by a :, a list of pointers to where the data for the keyword 
            exists in the data file
        INPUT: 
        OUTPUT: 
    """
    index_fullpath = get_index_fullpath(table_name, keyword, 'keyword')
    
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
    write_index_map_keyword(table_name, keyword, index_fullpath)
    return index_fullpath

def exists_index_file_keyword(table_name, keyword):
    """EXISTS_INDEX_FILE_KEYWORD
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
    return os.path.exists(get_index_fullpath(table_name, keyword, 'keyword'))

def get_index_keyword_key_value(line):
    """GET_INDEX_KEYWORD_KEY_VALUE
        DESCRIPTION: 
        INPUT: 
        OUTPUT: 
    """
        
    # get rid of newline
    line = line.rstrip('\n')
    
    # split line over ':'
    # first part is dict key, second part is list of line pointers
    line_split = line.split(':')
    
    # It's possible to have more than one : in a line -- this happens when
    # the keyword itself has a : in it. So: assume that the last : is the meaningful
    # one and everything to the right is simple the byte list
    
    raw_pointers = line_split[len(line_split) - 1]
    attr_value = ':'.join(line_split[0:len(line_split) -1])
    
    str_pointers = raw_pointers.split(',')
    
    # Convert pointers from str to int
    pointers = []
    for p in str_pointers:
        pointers.append(int(p))
    
    return (attr_value, pointers)
    

def read_index_file_keyword(table_name, keyword):
    """READ_INDEX_FILE_KEYWORD
        DESCRIPTION: For a given table_name and keyword (attribute), open the corresponding
            keyword index file. For each line, add the keyword into a dict as a key, and
            the following numbers on the line as the dict values. This represents an
            byte number index for each keyword in the table.
        INPUT: 
            - table_name
            - keyword
        OUTPUT: 
    """
    index_results_dict = {}
    index_fullpath = get_index_fullpath(table_name, keyword, 'keyword')
    
    f = open(index_fullpath, 'r')
    for line in f:
        (k, pointers) = get_index_keyword_key_value(line)
        if k not in index_results_dict:
            # it shouldn't already be in there, but might as well check...
            index_results_dict[k] = pointers
    f.close()
    
    return index_results_dict

def value_constraint_has_index(Q, I, table_name):
    result = False
    # Loop through value_constraints, find a matching table.attr index file
    if table_name in Q.value_constraints:
        for c in Q.value_constraints[table_name]:
            ta = Q.WHERE[c]['Subject']
            ta_split = parse_table_attribute_pair(ta)
            if table_name == ta_split[0]:
                # now see if there's an index corresponding to an attribute
                if exists_index_file_keyword(ta_split[0], ta_split[1]) == True:
                    return True
    
    return result

def join_constraint_has_index(q, table_name='', attr_name=''):
    # Loop through value_constraints, find a matching table.attr index file
    if exists_index_file_keyword(table_name, attr_name) == True:
        return True
    
    return False

    

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
    
    # get index # in attribute_list of keyword
    table_attr_pair = combine_table_attribute_pair(table_name, keyword)
    k = get_attribute_index(table_attr_pair)
    
    # Create index in memory, write at the end
    index_results_dict = {}
    
    # b is the byte position in the file
    b = 0
    
    with open(csv_fullpath, 'rb') as f:
        while True:
            f.seek(b)
            (b_returned, line) = readline_like_csv(f)
            if not line:
                # No more lines in file
                break
            # Assume the first row is the header. Skip it.
            # Also skip if it's a blank row.
            if b > 0 and line.strip() != '':
                for row in csv.reader([line]):
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
                    
    # Write results to file
    print(write_index_file_keyword(table_name, keyword, index_results_dict))    
    # END TIMER - after writing index
    print("--- %s seconds ---" % (time.time() - start_time))

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

class IndexManager:
    def __init__(self):
        # Read available indexes in /index/
        self.master_index_dict = self.build_master_index_dict()
        # query_index_dict will hold index data for each relevant table_alias.attr_name
        self.query_index_dict = {}
        # bytelist dict will hold byte list for each table_alias.
        # This will direct the query to which byte positions to read from file.
        # Note: a byte list may exist even if an index for the table_alias does not.
        self.value_bytelist_dict = {}
        self.bytelist_dict = {}
        
    def build_master_index_dict(self):
        # List of all available indexes in system
        # TODO: want to remove dependence on master_index_dict
        #dict[table_name][attr] = [Index_list]
    
        index_dict_all = get_index_list_all2()
        master_index_dict = {}
        for table_name in index_dict_all:
            master_index_dict[table_name] = {}
            for attr_name in index_dict_all[table_name]:
                master_index_dict[table_name][attr_name] = []
                for f in index_dict_all[table_name][attr_name]:
                    index_type = parse_index_filename(f)[1]
                    master_index_dict[table_name][attr_name].append(Index(table_name, attr_name, index_type))
                    
        return master_index_dict

        
    def get_index_priority_list(self):
        # If multiple types of indexes exist for a table, get the best
        return ['query-filter', 'hash', 'keyword']
        

    def load_blank_indexes(self, Q):
        # Load a blank index for each table_alias called out in WHERE clause
        index_dict = self.query_index_dict
        for table_attr in Q.where_table_attribute_list:
            (table_alias, attr_name) = parse_table_attribute_pair(table_attr)
            if table_alias not in index_dict:
                index_dict[table_alias] = {}
            if attr_name not in index_dict[table_alias]:
                # Initialize to None, not empty list. Default None will signify that no
                # index exists for table_attr; empty list will signify empty index (no pointers)
                index_dict[table_alias][attr_name] = None
            
            # Add empty Index for each table_alias.attr_name into self.index_dict
            index = self.find_best_index(table_alias, attr_name, Q)
            if index != None:
                index_dict[table_alias][attr_name] = index
            
                # Load index map into memory
                index.load_map()

        test_print('load_indexes / index_dict',index_dict)
        self.query_index_dict = index_dict
        self.full_query_index()     # True if all WHERE table_alias.attr_name have an index
        test_print('load_indexes / full_query_index',self.full_index)
        

    def load_value_constraint_indexes(self, Q):
        # For each value constraint, load the index for that attr_value into memory
        # Intended for use at startup
        for table_alias in Q.value_constraints:
            for c in Q.value_constraints[table_alias]:
                # Get the attr_value for this constraint
                table_alias_attr = Q.WHERE[c]['Subject']
                attr_name = parse_table_attribute_pair(table_alias_attr)[1]
                
                attr_value = Q.WHERE[c]['Object']
                comparison = Q.WHERE[c]['Verb']
                
                # The equal to case is easiest
                if comparison == '=':
                    self.load_index_by_value(table_alias, attr_name, attr_value)
                else:
                    # Otherwise, need to evaluate different entries in index_map
                    # Identify index
                    index = self.query_index_dict[table_alias][attr_name]
                    for attr_value_map in index.map:
                        if eval_binary_comparison(attr_value_map, comparison, attr_value) == True:
                            self.load_index_by_value(table_alias, attr_name, attr_value_map)
                        # TODO: could shortcut this since .map is sorted - 
                        #   say, for <, once you go greater/equal, can stop reading
                        
    def load_join_constraint_indexes(self, Q):
        for (table_alias1, table_alias2) in Q.join_constraints:
            for c in Q.join_constraints[(table_alias1, table_alias2)]:
                attr_name1 = parse_table_attribute_pair(Q.WHERE[c]['Subject'])[1]
                attr_name2 = parse_table_attribute_pair(Q.WHERE[c]['Object'])[1]
                # Assumption: indexes have not been loaded on join constraint attributes
                #   i.e., don't check, just load
                
                # TODO: let's see what happens if we just load the front half of the join_constraint.
                #   I think the second half will be taken care of via index map
                index1 = self.query_index_dict[table_alias1][attr_name1]
                for attr_value_map1 in index1.map:
                    self.load_index_by_value(table_alias1, attr_name1, attr_value_map1)
    
    def load_index_by_value(self, table_alias, attr_name, attr_value):
        # Instead of loading the entire index at once, only get the index for the values
        # you need as you need them
        self.query_index_dict[table_alias][attr_name].load_index_value(attr_value)
        #print(self.query_index_dict[table_alias][attr_name].index_dict)
        
            
    def find_best_index(self, table_alias, attr_name, Q):
        # un-alias table_alias into table_name
        for alias_name in Q.alias:
            if alias_name == table_alias:
                table_name = Q.alias[table_alias]
                break
    
        if table_name in self.master_index_dict:
            if attr_name in self.master_index_dict[table_name]:
                for index in self.master_index_dict[table_name][attr_name]:
                    for p in self.get_index_priority_list():
                        if index.type == p:
                            return index
        
        return None
            
    def full_query_index(self):
        # Set self.full_index if all WHERE table_alias.attr_name pairs have an index
        for table_name in self.query_index_dict:
            for attr_name in self.query_index_dict[table_name]:
                if self.query_index_dict[table_name][attr_name] == None:
                    self.full_index = False
        self.full_index = True


    def check_bytelist(self, table_alias):
        # Simply: True if a bytelist exists for this table_alias
        result = False
        if table_alias in self.bytelist_dict:
            if self.bytelist_dict[table_alias] != None:
                result = True
        return result
        
    def build_bytelists(self, Q):
        # For each table_alias in WHERE, instantiate a byte list if index exists
        # TODO: this might be throwing out index values if there are some arithmetic terms
        #   appended to the join constraints...
        
        # Create self.value_bytelist_dict entries for each table_alias that has a value constraint
        for table_alias in Q.value_constraints:
            if table_alias not in self.query_index_dict:
                # No index for this table_alias -- set to None
                self.value_bytelist_dict[table_alias] = None
            else:
                # If the index exists for table, want to find index for attr_name
                # for the value constraints. If that index exists, byte list will be the 
                # list of byte positions for the particular attr_value in value constraint
                if table_alias not in self.value_bytelist_dict:
                    #test_print('build_bytelists / create new one', table_alias)
                    self.value_bytelist_dict[table_alias] = ByteList(table_alias)
                # Get the bytelist based on the value constraints
                self.value_bytelist_dict[table_alias].get_value_constraint_bytelist(Q, self)
                self.bytelist_dict[table_alias] = self.value_bytelist_dict[table_alias]
        
        # Table_aliases that also have a join constraint are treated differently. If a 
        # value bytelist exists for this table, it will be filtered by the join constraints
        
        for (table_alias1, table_alias2) in Q.join_constraints:
            # TODO: Currently assuming queries with an index for all table_alias.attr_name pairs
            #       i.e. self.full_index == True
            # TODO: failing on a straight join with no value constraints
                
            # Initialize bytelist_dict[table_alias] if needed
            if table_alias1 not in self.bytelist_dict:
                self.bytelist_dict[table_alias1] = ByteList(table_alias1)
                self.bytelist_dict[table_alias1].bytes = []
            if table_alias2 not in self.bytelist_dict:
                self.bytelist_dict[table_alias2] = ByteList(table_alias2)
                self.bytelist_dict[table_alias2].bytes = []
                
            for c in Q.join_constraints[(table_alias1, table_alias2)]:
                attr_name1 = parse_table_attribute_pair(Q.WHERE[c]['Subject'])[1]
                index1 = self.query_index_dict[table_alias1][attr_name1]
                attr_name2 = parse_table_attribute_pair(Q.WHERE[c]['Object'])[1]
                index2 = self.query_index_dict[table_alias2][attr_name2]
                
                # 0) Reorder table_alias1 and table_alias2 if bytelist1 is longer than bytelist2.
                # Also requires that there is a value_constraint on table_alias2, otherwise
                # you'll get a false comparison on having a smaller bytelist2.
                # The idea is to minimize the number of loops run to get the full bytelists
                # to check, and table1 is the basis for the outer loop.
                flip_table_alias1 = table_alias1
                flip_table_alias2 = table_alias2
                flip_attr_name1 = attr_name1
                flip_attr_name2 = attr_name2
                flip_index1 = index1
                flip_index2 = index2
                flip_comparison = Q.WHERE[c]['Verb']
                flip = False
                if table_alias2 in Q.value_constraints:
                    if len(self.bytelist_dict[table_alias1].bytes) > len(self.bytelist_dict[table_alias2].bytes):
                        flip = True
                        flip_table_alias1 = table_alias2
                        flip_table_alias2 = table_alias1
                        flip_attr_name1 = attr_name2
                        flip_attr_name2 = attr_name1
                        flip_index1 = index2
                        flip_index2 = index1
                    
                    # TODO: Need to flip other comparisons that are not =
                
                # 1) Find set of attr_value1 from index related to value_bytelist
                #       Find attr_value1 from the reverse_index_dict
                attr_value_list1 = []
                join_bytelist_dict1 = {}
                join_bytelist1 = []
                join_bytelist2 = []
                
                for b in self.bytelist_dict[flip_table_alias1].bytes:
                    # index1 already has the indexes loaded for the value_constraints 
                    # attribute values on this table_alias--but only these attribute
                    # values. Other values need to be pulled from data tables.
                    # TODO: Would not need to hit data tables if we were hashing.
                    if b not in flip_index1.reverse_index_dict:
                        # Need to find that attr_value1 another way
                        # and load the index for that attr_value1 
                        #attr_value1 = index1.scan_map_for_value(b)
                        attr_value1 = flip_index1.get_attribute_value(b, flip_table_alias1, Q)
                        flip_index1.load_index_value(attr_value1)
                        
                        # Put the index in the IndexManager
                        self.query_index_dict[flip_table_alias1][flip_attr_name1] = flip_index1
                            
                        
                    else:
                        attr_value1 = flip_index1.reverse_index_dict[b]
                        
                    if attr_value1 not in attr_value_list1:
                        attr_value_list1.append(attr_value1)
                    if attr_value1 not in join_bytelist_dict1:
                        join_bytelist_dict1[attr_value1] = []
                    join_bytelist_dict1[attr_value1].append(b)
                    #test_print('build_bytelists / 1 / join_bytelist_dict1[attr_value1]', join_bytelist_dict1[attr_value1])
                    #test_print('build_bytelists / 1 / self.bytelist_dict[table_alias1]', self.bytelist_dict[table_alias1])
                
                # 2) Find bytelists from table_alias2 for all attr_value in attr_value_list1
                for attr_value2 in attr_value_list1:
                    if attr_value2 not in flip_index2.index_dict:
                        # If the index is not loaded for this attr_value, use the index map
                        # to get the index
                        if attr_value2 in flip_index2.map:
                            # load_index_value uses index2.map to load the index for this
                            # particular value
                            flip_index2.load_index_value(attr_value2)
                            # Put the index in the IndexManager
                            self.query_index_dict[flip_table_alias2][flip_attr_name2] = flip_index2
                        else:
                            # If it's not in index2.map, then the value is not in table2,
                            # which means it doesn't join. Remove it from the left side of the join
                            # and go to the next value in the for loop
                            del join_bytelist_dict1[attr_value2]
                            continue
                    
                    # After getting index2 set up, add b from index2 to join bytelist2
                    for b in flip_index2.index_dict[attr_value2]:
                        if b not in join_bytelist2:
                            join_bytelist2.append(b)
                    
                #test_print('build_bytelists / 2 / join_bytelist_dict1',(table_alias1, join_bytelist_dict1))
                #test_print('build_bytelists / 2 / join_bytelist2',(table_alias2, join_bytelist2))
                        
                # 3) Flatten byte positions from join_bytelist_dict into join_bytelist1
                for attr_value1 in join_bytelist_dict1:
                    for b in join_bytelist_dict1[attr_value1]:
                        if b not in join_bytelist1:
                            join_bytelist1.append(b)
                #test_print('build_bytelists / 3 / join_bytelist1',(table_alias1, join_bytelist1))
                            
                # 4) Combine join_bytelist into bytelist_dict for 1 and 2
                #   there is no combination logic to apply

                connector = Q.WHERE[c]['Connector']
                if connector == '':
                    self.bytelist_dict[flip_table_alias1].bytes = join_bytelist1
                    self.bytelist_dict[flip_table_alias2].bytes = join_bytelist2
                else:
                    temp_bytelist1 = []
                    temp_bytelist2 = []
                    
                    if connector == 'OR':
                        temp_bytelist1 = self.bytelist_dict[flip_table_alias1].bytes
                        for b in join_bytelist1:
                            if b not in temp_bytelist1 or len(self.bytelist_dict[flip_table_alias1].bytes) == 0:
                                temp_bytelist1.append(b)
                        temp_bytelist2 = self.bytelist_dict[flip_table_alias2].bytes
                        for b in join_bytelist2:
                            if b not in temp_bytelist2 or len(self.bytelist_dict[flip_table_alias2].bytes) == 0:
                                temp_bytelist2.append(b)
                                
                    elif connector == 'AND':
                        for b in join_bytelist1:
                            if b in self.bytelist_dict[flip_table_alias1].bytes or len(self.bytelist_dict[flip_table_alias1].bytes) == 0:
                                temp_bytelist1.append(b)
                        for b in join_bytelist2:
                            if b in self.bytelist_dict[flip_table_alias2].bytes or len(self.bytelist_dict[flip_table_alias2].bytes) == 0:
                                temp_bytelist2.append(b)

                    elif connector == 'NOT':
                        for b in join_bytelist1:
                            if b not in self.bytelist_dict[flip_table_alias1].bytes or len(self.bytelist_dict[flip_table_alias1].bytes) == 0:
                                temp_bytelist1.append(b)
                        for b in join_bytelist2:
                            if b not in self.bytelist_dict[flip_table_alias2].bytes or len(self.bytelist_dict[flip_table_alias2].bytes) == 0:
                                temp_bytelist2.append(b)
                    
                    self.bytelist_dict[flip_table_alias1].bytes = temp_bytelist1
                    self.bytelist_dict[flip_table_alias2].bytes = temp_bytelist2


class Index:
    def __init__(self, table_name, attr_name, type):
        self.type = type
        self.table_name = table_name
        self.attr_name = attr_name
        self.table_attr = combine_table_attribute_pair(table_name, attr_name)
        self.filepath = get_index_fullpath(table_name, attr_name, type)
        self.index_dict = {}
        self.reverse_index_dict = {}
        self.map = {}
    

    def load_index_value(self, attr_value):
        # Given an attr_value, load the lines for that attr_value into memory

        pointers = get_pointers_from_index_keyword(self.table_name, self.attr_name, self.map, attr_value)
        #print('pointers:',pointers)

        # Add to index_dict
        #if attr_value not in self.index_dict:
        self.index_dict[attr_value] = pointers

        # Add to reverse_index_dict
        for p in pointers:
            # pointers should come back as a list of integers
            self.reverse_index_dict[p] = attr_value
            
        #print('self.index_dict:', self.index_dict)
        #print('self.reverse_index_dict:', self.reverse_index_dict)

        
    def load_map(self):
        # Load the index map into memory
        self.map = get_index_map_keyword(self.table_name, self.attr_name)
        
        
    def scan_map_for_value(self, b):
        # Map format: attr_value:b_map
        # Only maps the first b of the pointers
        
        result = None
        b_map_prev = 0
        for attr_value in self.map:
            b_map = self.map[attr_value]
            if b_map == b:
                result = attr_value
                break
            elif b_map > b:
                # Went by it, so find the attr_value for b_map_prev
                result = self.scan_map_for_value(b_map_prev)
                break
            b_map_prev = b_map

        return result
        
    def get_attribute_value(self, b, table_alias, Q):
        # Go right to the data file, pull the value for a particular attr_name
        csv_fullpath = table_to_csv_fullpath(self.table_name)
        f = open(csv_fullpath, 'rb')
        f.seek(b)
        (b_returned, line) = readline_like_csv(f)
        f.close()
        
        # Get value from line
        table_alias_attr = combine_table_attribute_pair(table_alias, self.attr_name)
        attr_index = Q.get_attribute_dict_index(table_alias_attr)
        
        for row in csv.reader([line]):
            attr_value = row[attr_index]
        
        #test_print('get_attribute_value / line',line)
        #test_print('get_attribute_value / attr_index',attr_index)
        #test_print('get_attribute_value / attr_vaule',attr_value)
                
        return attr_value
        
class ByteList:
    # Essentially: just a list of byte positions in a file to f.seek() and read
    def __init__(self, table_alias):
        self.table_alias = table_alias
        
        # self.bytes is a list: [b1, b2, ..., b3]
        self.bytes = None

    
    def get_single_bytelist_where(self, Q, I, table_alias, c):
        """GET_SINGLE_BYTELIST_WHERE
        DESCRIPTION: Return a list of byte position numbers based on a single WHERE 
            component. Combination of multiple components handled in get_index_bytelist()
        INPUT: 
            - Query Q:
            - IndexManager I:
            - string table_name:
            - string attr_name: 
            - int c: position in Q.WHERE, i.e., which constraint to find byte list for
        OUTPUT: list bytelist: list containing byte positions for this WHERE clause
        """
    
        bytelist = []
        table_attr_where = Q.WHERE[c]['Subject']
        [table_alias_where, attr_name_where] = parse_table_attribute_pair(table_attr_where)
        if table_alias == table_alias_where:
            # now see if there's an index corresponding to an attribute
            if table_alias in I.query_index_dict:
                if attr_name_where in I.query_index_dict[table_alias]:

                    # TODO: there has to be a better way to reference an index..........
                    if I.query_index_dict[table_alias][attr_name_where] != None:
                        index = I.query_index_dict[table_alias][attr_name_where]
                        obj = Q.WHERE[c]['Object']
                        op = Q.WHERE[c]['Verb']

                        for attr_value in index.index_dict:
                            # Test if the attribute value meets the constraint
                            if eval_binary_comparison(attr_value, op, obj) == True:
                                for i in index.index_dict[attr_value]:
                                    try:
                                        bytelist.append(int(i))
                                    except:
                                        continue
    
        return bytelist
    
    def get_value_constraint_bytelist(self, Q, I):
        """
        DESCRIPTION: Return a list of byte position numbers for value_constraints
        INPUT: 
            - Query Q:
            - IndexManager I:
            - string table_name:
            - string attr_name: Name of attribute to find an index for. Should be empty for
                value_constraint index in order to find constraints for all attributes in 
                table. Should have a value for finding index on joins because that will be
                the specific joining attribute to find an index for.
            - string attr_value: 
        OUTPUT: list index_bytelist: list containing byte positions in table_name that 
            correspond to index values.
        """

        # TODO: What happens if this returns an empty list? Should there be a different signal
        #   that is returned if the index value isn't found in the index?
        self.bytes = []
    
        if self.table_alias in Q.value_constraints:
            # OK. Not all attributes in table may have an index. So need to determine which
            # ones do, and merge the byte lists for those (otherwise funny results are
            # produced trying to merge for indexes that don't exist...)
            vc = []
            for i in Q.value_constraints[self.table_alias]:
                # Get attr_name from corresponding WHERE constraint
                attr_name = parse_table_attribute_pair(Q.WHERE[i]['Subject'])[1]
                if I.query_index_dict[self.table_alias][attr_name] != None:
                    vc.append(i)
            
            # Add the first one, vc[0], if it exists -- no merging involved
            if len(vc) > 0:
                self.bytes = self.get_single_bytelist_where(Q, I, self.table_alias, vc[0])
        
            # If there are more than one value_constraint on this table, time to merge them
            for i in range(1,len(vc)):
                c = vc[i]
                this_bytelist = self.get_single_bytelist_where(Q, I, self.table_alias, c)
                temp_bytelist = []     # hold the temporary result
            
                # Connect the two constraints using the boolean connector on the second one
                operator = Q.WHERE[c]['Connector']
                if operator == 'OR':
                    temp_bytelist = self.bytes
                    for b in this_bytelist:
                        if b not in temp_bytelist:
                            temp_bytelist.append(b)
                elif operator == 'AND':
                    for b in this_bytelist:
                        if b in self.bytes:
                            temp_bytelist.append(b)
                elif operator == 'NOT':
                    for b in this_bytelist:
                        if b not in self.bytes:
                            temp_bytelist.append(b)
            
                # Replace full_bytelist with new list, temp_bytelist
                self.bytes = temp_bytelist
        else:
            self.bytes = None