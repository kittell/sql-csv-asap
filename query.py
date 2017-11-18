import utils
import csv


def get_select_values(row, query_select, attribute_dict, checking_table_name):
    """GET_SELECT_VALUES
    DESCRIPTION: This is the projection step of the query. Loop through SELECT query. 
        Determine which attributes to get from table. Append attribute values to 
        list returned from function.
    INPUT: 
        - row: row of data from csv file
        - query_select: full SELECT component of SQL query
        - attribute_dict: list of attributes, organized by table
        - checking_table_name: table holding attributes to select
    OUTPUT: select_list: list containing values from selected attributes
    """
    select_list = []

    # Loop through select query:
    #   Determine which attributes to get from this table
    #   Apply attribute values for this row to list
    
    #TODO: Consider appending to temp file instead of holding in memory
    
    # loop over select query
    for i in range(len(query_select)):
        # loop over attribute_dict - to get index numbers for attributes in data row
        for table_name in attribute_dict:
            if table_name == checking_table_name:
                # Parse table.attribute pair from query_select
                ta = utils.parse_table_attribute_pair(query_select[i])
                # get the index for the attribute you want to get value of
                for j in range(len(attribute_dict[table_name])):
                    if ta[1] == '*':
                        # if user chose to select all attributes via *, append value
                        select_list.append(row[j])
                    else:
                        # compare table name and attribute name; add if true
                        if ta[0] == table_name:
                            if ta[1] == attribute_dict[table_name][j]:
                                select_list.append(row[j])
                                break
    
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
                            if utils.eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                                result = True
                                break
    
    return result

def check_has_join(query_where):
    """CHECK_HAS_JOIN
    DESCRIPTION: Determine whether SQL query has join in WHERE conditions. Used as a
        flag for separate query result processing.
    INPUT: query_where: WHERE component of SQL query
    OUTPUT: result: True if WHERE contains join; otherwise, False
    """
    
    # query has a join in it if:
    #   1) both the subject and object of a WHERE term has a '.' in it; AND
    #   2) substrings on both sides of '.' are strings (i.e., not numbers)
    #   3) more than one table called in WHERE subject values (t1.attr, t2.attr)
    
    # TODO: This will find the . in a real number and say it's a join... fix that
    
    result = False
    has_dot = False
    table_list = []
    
    for i in range(len(query_where)):
        if '.' in query_where[i]['Subject']:
            if '.' in query_where[i]['Object']:
                has_dot = True
        ta = utils.parse_table_attribute_pair(query_where[i]['Subject'])
        if ta[0] != '':
            if ta[0] not in table_list:
                table_list.append(ta[0])
    
    if has_dot == True and len(table_list) > 1:
        result = True
    
    return result

def check_multi_where(query_where):
    """CHECK_MULTI_WHERE
    DESCRIPTION: Determines whether SQL query has multiple WHERE conditions. Used as a 
        flag for separate query result processing.
    INPUT: query_where: WHERE component of SQL query
    OUTPUT: result: True if multiple WHERE conditions; otherwise, False
    """
    
    # query has multiple WHERE if:
    #   1) len(query_where) > 1; AND
    #   2) any connector of a WHERE term is not an empty string (i.e., must have some
    #    logic to connect the conditions)
    
    result = False
    if len(query_where) > 1:
        for i in range(len(query_where)):
            if query_where[i]['Connector'] != '':
                result = True
                break
    
    return result

def combine_multi_where(int_select_values, int_where_results, query_where):
    """COMBINE_MULTI_WHERE
    DESCRIPTION: 
    INPUT: 
        - int_select_values: selected values that meet any WHERE condition
        - int_where_results: results for individual WHERE condition tests
            [(table_name, i_row), [w1, w2, ..., wn]]
        - query_where: full parsed WHERE query - will be using the Connector values, 
            which are the boolean connectors between the multiple WHERE conditions
    OUTPUT: result: rows from int_select_values that pass all WHERE conditions
    """
        
    #TODO: Consider appending the results to a temp file instead of holding in memory
    
    result = []
    
    # loop through each row of selected values
    for i in range(len(int_select_values)):
    
        # loop through each WHERE results, comparing pairs in order
        # TODO: Consider operator precedence, groups of conditions set off by parentheses
        where_result_list = []          # contains results of each pair comparison
        for w in range(len(query_where) - 1):
            op = query_where[w+1]['Connector']
            w1 = int_where_results[i][1][w]
            w2 = int_where_results[i][1][w+1]
            where_result = utils.eval_binary_comparison(w1, op, w2)
            where_result_list.append(where_result)
        
        # All individual tests must be True for final result to pass
        final_where_result = True
        if False in where_result_list:
            final_where_result = False
        
        # If this row passes all tests, add it to the final list
        if final_where_result == True:
            result.append(int_select_values[i])

    return result


def get_select_attributes(query_select):
    """GET_SELECT_ATTRIBUTES
    DESCRIPTION: Build a dict that will be used in other functions for comparing attribute
        values read from tables.
    INPUT: query_select: full SELECT component of SQL query
    OUTPUT: result: dict with key:value as table_name:[attributes]
    """
    result = {}
    
    # 1) Keys of dict will be the unique list of tables from SELECT query
    for attr in query_select:
        ta = utils.parse_table_attribute_pair(attr)
        result[ta[0]] = []
    
    # 2) append attributes to empty list in dict values
    for attr in query_select:
        ta = utils.parse_table_attribute_pair(attr)
        result[ta[0]].append(ta[1])
    
    return result


def get_where_results(row, query_where, attribute_dict, checking_table_name, i_row):
    """GET_WHERE_RESULTS
    DESCRIPTION: 
    INPUT: 
        - row: current row being read from .csv file
        - query_where: WHERE components of SQL query
        - checking_table_name: table where data row is being read from
        - i_row: row number of table
    OUTPUT: result: array that matches table/row with its WHERE condition test results
        [((table_name, i_row), [where_result])]
    """
    # TODO: This function might be able to replace/make unnecessary test_row()
    
    result = []
    where_result = []
    
    for i in range(len(query_where)):
        where_result.append(False)

        # find index of table.attribute being tested
        for table_name in attribute_dict:
            if table_name == checking_table_name:
                for j in range(len(attribute_dict[table_name])):
                    # combine into table.attr pair for comparison
                    ta = utils.combine_table_attribute_pair(table_name, attribute_dict[table_name][j])
                    
                    # TODO: this next part assumes that subject is attribute, and object is
                    #   an entered value. But it could be another attribute, as in a join
                    if query_where[i]['Subject'] == ta:
                        if utils.eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                            where_result[i] = True
                            break
    
    result = [(checking_table_name, i_row), where_result]
    
    return result

def perform_query(query):
    """PERFORM_QUERY
    DESCRIPTION: This is where the query work starts. Take a parsed SQL query, apply it
        to the selected tables, and return the final results.
    INPUT: query: dict with keys for SELECT, FROM, WHERE terms
    OUTPUT: final_select_results: list of resulting rows from query
    NOTES:
        - Assumption: SELECT and FROM are valid. They are the minimum to query.
    """

    # Get list of csv files from FROM query. These will be full paths to file.
    # TODO: Using full paths in anticipation of grabbing .csv files from internet
    csv_list = []
    for table in query['FROM']:
        csv_fullpath = utils.table_to_csv_fullpath(table)
        csv_list.append(csv_fullpath)
    
    # Initialize lists and dicts:
    int_where_results = []
    int_select_values = []
    final_select_results = []
    int_select_attributes = get_select_attributes(query['SELECT'])  # list of SELECTed attributes
    has_join = check_has_join(query['WHERE'])       # WHERE condition contains table join
    has_multi_where = check_multi_where(query['WHERE'])     # multiple WHERE conditions
    
    # Build attribute dict. Used to associate attribute names (row 1) with their data.
    attribute_dict = {}
    for csv_fullpath in csv_list:
        attribute_dict = utils.get_attribute_dict(attribute_dict, csv_fullpath)
    
    # READ DATA FROM TABLES
    for csv_fullpath in csv_list:
        table_name = utils.csv_fullpath_to_table(csv_fullpath)
        
        i_row = 1                   # count rows in each table, used for indexing
        with open(csv_fullpath, newline = '', encoding='utf-8') as f:
            r = csv.reader(f)
            next(r)                 # Assumption: first row is header. Skip it.
            for row in r:
                i_row = i_row + 1

                # decide whether this row passes any WHERE condition
                pass_where_condition = test_row(row, query['WHERE'], attribute_dict, table_name)
                if pass_where_condition == True:
                    # Get SELECTed attributes for this row if it passes
                    new_select_value = get_select_values(row, query['SELECT'], attribute_dict, table_name)
                    if len(new_select_value) > 0:
                        if has_multi_where == True:
                            # If there multiple WHERE conditions, test against each condition.
                            # Note: will combine the individual condition results later.
                            # new_where_results includes the results of the row against each.
                            new_where_results = get_where_results(row, query['WHERE'], attribute_dict, table_name, i_row)
                            int_select_values.append(new_select_value)
                            int_where_results.append(new_where_results)
            
                        else:
                            # if only 0 or 1 WHERE conditions, simply add the SELECTed values
                            final_select_results.append(new_select_value)
    
    # AFTER READING DATA FROM TABLES
    # If this query has multiple where statements, combine the individual results:
    if has_multi_where == True:
        utils.test_print('perform_query: has_multi_where', has_multi_where)
        final_select_results = combine_multi_where(int_select_values, int_where_results, query['WHERE'])
    
    return final_select_results