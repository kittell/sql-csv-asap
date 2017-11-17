import utils
import operator
import csv

TESTPRINT = utils.get_test_mode()

def get_comparison_function(c):
    #inspiration: https://stackoverflow.com/a/1740759/752784
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
            'NOT': operator.not_
        }[c]

        
def eval_binary_comparison(a, op, b):
    return get_comparison_function(op)(a, b)


def parse_table_attribute_pair(ta):
    # For a table.attr pair, split into [table, attr]
    # Assumption: zero or one dots
    if '.' in ta:
        result = ta.split('.')
    else:
        result = ['', ta]
    return result


def get_select_values(row, query_select, attribute_dict, checking_table_name):
    # Remember: compare table.attribute to checking_table_name
    select_list = []

    # This function happens after program determines row meets ANY WHERE condition
    # Loop through select query:
    #   Determine which attributes to get from this table
    #   Apply attribute values for this row to list
    
    #TODO: much better memory-wise if this is written to temp file instead of holding in 
    #   memory via list
    
    # loop over select query
    for i in range(len(query_select)):
        # loop over attribute_dict - to get index numbers for attributes in data row
        for table_name in attribute_dict:
            if table_name == checking_table_name:
                # Parse table.attribute pair from query_select
                ta = parse_table_attribute_pair(query_select[i])
                # get the index for the attribute you want to get value of
                for j in range(len(attribute_dict[table_name])):
                    if ta[1] == '*':
                        # if user chose to select all attributes via *, append value
                        select_list.append(row[j])
                    else:
                        # compare table name and attribute name; add if true
                        if ta[0] == table_name:
                            if ta[1] == attribute_dict[table_name][j]:
#                                if TESTPRINT == True:
#                                    print('select_list.append(row[j]):',row[j])
                                select_list.append(row[j])
                                break
    
#    if TESTPRINT == True:
#        print('select_list:',select_list)
    
    return select_list


def test_row(row, query_where, attribute_dict, checking_table_name):
    """
    DESCRIPTION: determines whether data row passes any WHERE condition
    INPUT:
        - row to test (list)
        - where criteria or none
    OUTPUT: true/pass; false
    """
    
    #TODO: How are we handling joins here?
    result = False
    
    if query_where == '':
        # If WHERE wasn't specified, don't test the row, just pass it
        result = True
    else:
        #loop through all WHERE conditions
        for i in range(len(query_where)):
            # find index of matching attribute
            for table_name in attribute_dict:
                if checking_table_name == table_name:
                    for j in range(len(attribute_dict[table_name])):
                        # combine into table.attr pair for comparison
                        ta = table_name + '.' + attribute_dict[table_name][j]
                        if query_where[i]['Subject'] == ta:
                            if eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                                result = True
                                break
    
#    if TESTPRINT == True:
#        print('test_row result:',result)
    
    return result

def check_has_join(query_where):
    # query has a join in it if:
    #   1) both the subject and object of a WHERE term has a '.' in it; AND
    #   2) more than one table called in WHERE subject values (t1.attr, t2.attr)
    
    result = False
    has_dot = False
    table_list = []
    
    for i in range(len(query_where)):
        if '.' in query_where[i]['Subject']:
            if '.' in query_where[i]['Object']:
                has_dot = True
        ta = parse_table_attribute_pair(query_where[i]['Subject'])
        if ta[0] != '':
            if ta[0] not in table_list:
                table_list.append(ta[0])
    
    if has_dot == True and len(table_list) > 1:
        result = True
    
    return result

def check_multi_where(query_where):
    # query has multiple WHERE if:
    #   1) len(query_where) > 1; AND
    #   2) any connector of a WHERE term is not an empty string (i.e., will have some logic)
    
    result = False
    if len(query_where) > 1:
        for i in range(len(query_where)):
            if query_where[i]['Connector'] != '':
                result = True
                break
    
    return result

def combine_multi_where(int_select_values, int_where_info, query_where):
    #int_where_info: [(table_name, i_row), [w1, w2, ..., wn]]
    
    #TODO: better to print to temp file than stuff into memory
    
    result = []
        
    for i in range(len(int_select_values)):
    
        # loop through each pair of WHERE results
        where_result_list = []
        for w in range(len(query_where) - 1):
            op = query_where[w+1]['Connector']
            w1 = int_where_info[i][1][w]
            w2 = int_where_info[i][1][w+1]
            where_result = eval_binary_comparison(w1, op, w2)
            if TESTPRINT == True:
                print('where_result:', where_result)
            where_result_list.append(where_result)
        
        final_where_result = True
        if False in where_result_list:
            final_where_result = False
            
        
        if TESTPRINT == True:
            print('final_where_result:', final_where_result)
        if final_where_result == True:
            result.append(int_select_values[i])

    return result


def get_select_attributes(query_select):
    #query_select is a list of table.attr pairs
    # return a dict of table:[attr list]
    result = {}
    
    #TODO: obvious redundancy here--fix it later
    
    # 1) start dict with unique tables
    for attr in query_select:
        ta = parse_table_attribute_pair(attr)
        result[ta[0]] = []
    
    # 2) add attributes
    for attr in query_select:
        ta = parse_table_attribute_pair(attr)
        result[ta[0]].append(ta[1])
    
    return result

def perform_query(query):
    # Assumption: SELECT and FROM are valid
    
    # Get list of csv files from FROM query.
    csv_list = []
    for table in query['FROM']:
        csv_list.append(utils.table_to_csv(table))
    
    int_where_results = []
    int_where_attributes = []
    int_where_info = []
    int_select_values = []
    int_select_rows = {}
    int_select_attributes = get_select_attributes(query['SELECT'])
    final_where_results = []
    final_select_results = []
    has_join = check_has_join(query['WHERE'])
    has_multi_where = check_multi_where(query['WHERE'])
    
    # Build attribute dict to associate attribute names with their tables
    attribute_dict = {}
    for csv_filename in csv_list:
        csv_fullpath = utils.get_csv_fullpath(csv_filename)
        attribute_dict = utils.get_attribute_dict(attribute_dict, csv_fullpath)
    
    i_selected = 0
    for csv_filename in csv_list:
        csv_fullpath = utils.get_csv_fullpath(csv_filename)
        #attribute_list = utils.get_attribute_list(csv_fullpath)
        table_name = utils.csv_to_table(csv_filename)
        i_table = 0
#        int_select_values[table_name] = []
#        int_select_rows[table_name] = []
        
        i_row = 0
        with open(csv_fullpath, newline = '', encoding='utf-8') as f:
            r = csv.reader(f)
            next(r)
            for row in r:
                i_row = i_row + 1
                if i_row > 1:
                    # skip the header row..........
                    # decide whether this row passes any WHERE conditions; if so, take the row
                    take_row = test_row(row, query['WHERE'], attribute_dict, table_name)
                    if take_row == True:
                        if has_multi_where == True:
                            # if there is a multiple where, need to grab multiple lists to compare
                            # will have a dict entry for each table
                            new_select_value = get_select_values(row, query['SELECT'], attribute_dict, table_name)
                            new_where_info = get_where_info(row, query['WHERE'], attribute_dict, table_name, i_row)
                            
                            if len(new_select_value) > 0:
                                int_select_values.append(new_select_value)
                                int_where_info.append(new_where_info)
                                i_selected = i_selected + 1
                    
#                            if TESTPRINT == True:
#                                print()
                                #print('int_select_values:',int_select_values)
#                                print('len(int_select_values):',len(int_select_values))
                                #print('int_where_info:',int_where_info)
#                                print('len(int_where_info:)',len(int_where_info))
#                                print('int_select_attributes:',int_select_attributes)
                
                        else:
                            final_select_results.append(get_select_values(row, query['SELECT'], attribute_dict, table_name))
    
    # if this query has multiple where statements, extra processing step:
    if has_multi_where == True:
        if TESTPRINT == True:
            print('combining multi where')
            #print('int_select_values:', int_select_values)
        final_select_results = combine_multi_where(int_select_values, int_where_info, query['WHERE'])
    
    return final_select_results

def get_where_info(row, query_where, attribute_dict, checking_table_name, i_row):
    # Result will be an array: [((table_name, i_row), [where_condition_results])]
    result = []
    where_result = []
    
    for i in range(len(query_where)):
        where_result.append(False)
        found_result = False
        # find index of matching attribute
        for table_name in attribute_dict:
            if table_name == checking_table_name:
                for j in range(len(attribute_dict[table_name])):
                    # combine into table.attr pair for comparison
                    ta = table_name + '.' + attribute_dict[table_name][j]
                    # TODO: this next part assumes that subject is attribute, and object is
                    #   an entered value. But it could be another attribute, as in a join
                    if query_where[i]['Subject'] == ta:
                        if eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                            where_result[i] = True
                            break
    result = [(checking_table_name, i_row), where_result]

#    if TESTPRINT == True:
#        print('get_where_info:', result)
    
    return result