import utils
import csv
import os

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
    #   Testing 2017-11-19: If it's a join, skip it. Handle it in separate function.
    #       This function is for testing the comparison-type WHERE queries
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
    DESCRIPTION: Test a given row of csv data against all WHERE conditions.
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
                        
                    if query_where[i]['Subject'] == ta:
                        if query_where[i]['Join'] == False:
                            # If this WHERE clause is not a join across tables, simply do the test
                            if utils.eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                                where_result[i] = True
                                break
                        else:
                            # TODO: if this is a join........
                            other_checking_ta = utils.parse_table_attribute_pair(query_where[i]['Object'])
                            other_checking_table_name = other_ta[0]
                            other_csv_fullpath = utils.table_to_csv_fullpath(other_checking_table_name)
                            
                            # open the other csv
                            # run through the data, doing a test on the appropriate attribute
                            # when a match is found, return true
                            # bonus: write to a temp file to match the subject:object join row ids:
                            #   temp filename: temp_where_[i]_join.csv
                            #   row format: table1, table1_row, table2, table2_row
                            
                            with open(other_csv_fullpath, newline = '', encoding='utf-8') as f2:
                                r2 = csv.reader(f2)
                                next(r2)                 # Assumption: first row is header. Skip it.
                                for row2 in r2:
                                    #TODO: temp fix for reading blank row; more generally, for reading row with fewer
                                    #   data entries in a row than number of attributes in header
                                    if ''.join(row2).strip() == '':
                                        continue
                                    # get index for attribute value in other_table_name
                                    for other_table_name in attribute_dict:
                                        if other_table_name == other_checking_table_name:
                                            for j2 in range(len(attribute_dict[other_table_name])):
                                                other_ta = utils.combine_table_attribute_pair(other_table_name, attribute_dict[other_table_name][j])
                                                if query_where[i]['Subject'] == other_ta:
                                                    if utils.eval_binary_comparison(row2[j], query_where[i]['Verb'], query_where[i]['Subject']):
                                                        where_result[i] = True
                                                        break
                                            
                            where_result[i] = None  # TODO - a very temporary measure until joining works
                            break
    
    result = [(checking_table_name, i_row), where_result]
    
    return result

def perform_query(query):
    """PERFORM_QUERY
    DESCRIPTION: This is where the query work starts. Take a parsed SQL query, apply it
        to the selected tables, and return the final results.
    INPUT: query: dict with keys for SELECT, FROM, WHERE terms
    OUTPUT: final_select_results: list of resulting data values from query
    NOTES:
        - Assumption: SELECT and FROM are valid. They are the minimum to query.
    """
    
    # query format:
    #           query[WHERE][i]['Connector']
    #           query[WHERE][i]['Subject']
    #           query[WHERE][i]['Verb']
    #           query[WHERE][i]['Object']
    #           query[WHERE][i]['Join']

    final_select_results = []
    
    # Get list of csv files from FROM query. These will be full paths to file.
    csv_list = []
    for table in query['FROM']:
        csv_fullpath = utils.table_to_csv_fullpath(table)
        csv_list.append(csv_fullpath)
    
    # Build attribute dict. Used to associate attribute names (row 1) with their data.
    attribute_dict = {}
    for csv_fullpath in csv_list:
        attribute_dict = utils.get_attribute_dict(attribute_dict, csv_fullpath)
    
    utils.test_print('perform_query / attribute_dict', attribute_dict)
    
    # Determine which WHERE clauses are joins or attribute constraints
    where_joins = []
    where_constraints = {}
    for i in range(len(query['WHERE'])):
        utils.test_print('perform_query / i', i)
        if query['WHERE'][i]['Join'] == True:
            where_joins.append(i)
            utils.test_print('perform_query / where_joins', where_joins)
        else:
            ta = utils.parse_table_attribute_pair(query['WHERE'][i]['Subject'])
            utils.test_print('perform_query / ta', ta)
            if ta[0] not in where_constraints:
                where_constraints[ta[0]] = []
            where_constraints[ta[0]].append(i)
            utils.test_print('perform_query / where_constraints', where_constraints)
    
    utils.test_print('perform_query / where_joins', where_joins)
    utils.test_print('perform_query / where_constraints', where_constraints)
    
    # Walk through joins first.
    # For a pair of joins from table1 to table2, walk through table1 and find the
    # matching row in table2. Apply attribute constraints to each side.
    # Write the result to a temp file: join_map_[i].csv in the format;
    # table1, row1, table2, row2.
    
    # TODO: wipe temp files that may exist
    
    # TODO: do it like this: for each joined pair:
    #   1) apply attribute constraints to table 1 row
    #   2) apply table1-table2 join
    #   3) apply attribute constraints to table 2 row
    
    if len(where_joins) > 0:
        for i in range(len(where_joins)):
            ta1 = utils.parse_table_attribute_pair(query['WHERE'][i]['Subject'])
            ta2 = utils.parse_table_attribute_pair(query['WHERE'][i]['Object'])
            csv1 = utils.table_to_csv_fullpath(ta1[0])
            csv2 = utils.table_to_csv_fullpath(ta2[0])
            
            # Get index for join attributes to test
            a1 = utils.get_attribute_index(ta1, attribute_dict)
            a2 = utils.get_attribute_index(ta2, attribute_dict)
                        
            with open(csv1, newline = '', encoding = 'utf-8') as f1:
                i_row1 = 1
                r1 = csv.reader(f1)
                next(r1)            # Assumption: first row is header. Skip it.
                
                for row1 in r1:
                    i_row1 = i_row1 + 1
                    # Skip over blank rows
                    if ''.join(row1).strip() == '':
                        continue
                    
                    # Before diving into join tests, see if this row meets the 
                    # attribute constraints
                    
                    constraints_list = []
                    constraint = 0
                    for j in where_constraints[ta1[0]]:
                        constraint = constraint + 1
                        if constraint > 1:
                            constraints_list.append(query['WHERE'][j]['Connector'])
                    
                        c1 = utils.parse_table_attribute_pair(query['WHERE'][j]['Subject'])
                        c1_index = utils.get_attribute_index(c1, attribute_dict)
                        op = query['WHERE'][j]['Verb']
                        obj = query['WHERE'][j]['Object']
                        constraints_list.append(utils.eval_binary_comparison(row1[c1_index], op, obj))
                    
                    
                    pass_constraints = constraints_list[0]
                    if len(constraints_list) > 2:
                        for j in range(len(constraints_list)):
                            if j % 2 == 0 and j < len(constraints_list) - 2:
                                a = constraints_list[j]
                                op = constraints_list[j + 1]
                                b = constraints_list[j + 2]
                                pass_constraints = utils.eval_binary_comparison(a, op, b)
                        
                    
                    if pass_constraints == True:
                        v1 = row1[a1].strip()
                        if v1 == '':
                            # don't join null values
                            continue
                        utils.test_print('perform_query / v1', v1)
                        with open(csv2, newline = '', encoding = 'utf-8') as f2:
                            i_row2 = 1
                            r2 = csv.reader(f2)
                            next(r2)        # Assumption: first row is header. Skip it.
                        
                            for row2 in r2:
                                i_row2 = i_row2 + 1
                                # Skip over blank rows
                                if ''.join(row2).strip() == '':
                                    continue
                            
                                # TODO: apply attribute constraints on t2
                            
                                v2 = row2[a2].strip()
                                if v1 == v2:
                                    utils.test_print('perform_query / v2', v2)
                                    append_join_pair(i, ta1[0], i_row1, ta2[0], i_row2)
                                    break
                        f2.closed
            f1.closed
                            
    # After walking through joins, match map to data
            
    
    return final_select_results

def append_join_pair(i, table1, i_row1, table2, i_row2):
    
    filedir = utils.get_table_directory()
    filename = 'join_map_' + str(i) + '.csv'
    filepath = os.path.join(filedir, filename)
    
    writeline = [table1, i_row1, table2, i_row2]
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)