import utils
import csv
import os

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
    
    #utils.test_print('perform_query / attribute_dict', attribute_dict)
    
    # Determine which WHERE clauses are joins or attribute constraints
    join_constraints = map_join_constraints(query['WHERE'])
    value_constraints = map_value_constraints(query['WHERE'])
    
    # Walk through joins first.
    # For a pair of joins from table1 to table2, walk through table1 and find the
    # matching row in table2. Apply attribute constraints to each side.
    # Write the result to a temp file: join_map_[i].csv in the format;
    # table1, row1, table2, row2.
    
    # wipe temp files that may exist - b/c we're appending temp results, not overwriting
    # TODO: this should also happen at the end, except in test conditions where you want to see int results
    utils.remove_temp_files()
    
    # TODO: do it like this: for each joined pair:
    #   For row1 in each table_a in value_constraints:
    #       If row1 passes value_constraints[table_a]:
    #           If there are any join_constraints including table_a-table_b
    #               For row2 in each table_b:
    #                   If row2 passes value_constraints[table_b]
    
    # TODO: be smart about joins: b/c you walk table2 so often, make sure it's the smaller table
    
    table_list = []
    for table in query['FROM']:
        table_list.append(table)
    
    for table1 in table_list:
        if table1 not in join_constraints:
            # if there are no joins, just apply value_constraints to single table
            csv1 = utils.table_to_csv_fullpath(table1)
            with open(csv1, newline = '', encoding = 'utf-8') as f1:
                utils.test_print('perform_query / open(csv1)', csv1)
                i_row1 = 1
                r1 = csv.reader(f1)
                next(r1)            # Assumption: first row is header. Skip it.
                for row1 in r1:
                    i_row1 = i_row1 + 1
                    # Skip over blank rows
                    if ''.join(row1).strip() == '':
                        continue
                    # test row1 against value_constraints[table1]
                    pass_value_constraints = test_value_constraints(table1, row1, value_constraints, query['WHERE'], attribute_dict)
                    if pass_value_constraints:
                        print(row1) #TODO: printing is temporary
            f1.closed
        else:
            for table_pair in join_constraints:
                if table1 == table_pair[0]:
                    table2 = table_pair[1]
                    csv1 = utils.table_to_csv_fullpath(table1)
                    csv2 = utils.table_to_csv_fullpath(table2)
                    with open(csv1, newline = '', encoding = 'utf-8') as f1:
                        utils.test_print('perform_query / open(csv1)', csv1)
                        i_row1 = 1
                        r1 = csv.reader(f1)
                        next(r1)            # Assumption: first row is header. Skip it.
                        for row1 in r1:
                            i_row1 = i_row1 + 1
                            # Skip over blank rows
                            if ''.join(row1).strip() == '':
                                continue
                            # test row1 against value_constraints[table1]
                            pass_value_constraints = test_value_constraints(table1, row1, value_constraints, query['WHERE'], attribute_dict)
                            if pass_value_constraints:
                                pass_join_constraints = test_join_constraints(table, row, value_constraints, query_where, attribute_dict)
                                print(row1) #TODO: printing is temporary
                    f1.closed
    
    return final_select_results

def map_join_constraints(query_where):
    """MAP_JOIN_CONSTRAINTS
        DESCRIPTION: 
        INPUT:
            - query_where: WHERE component of parsed SQL query
        OUTPUT: join_constraints: dict of list; key of dict is a tuple of (table1, table2);
            value of idct is a list of index numbers from main WHERE query containing 
            constraint relevant to this table-table pair
    """
    join_constraints = {}
    for i in range(len(query_where)):
        if query_where[i]['Join'] == True:
            ta1 = utils.parse_table_attribute_pair(query_where[i]['Subject'])
            ta2 = utils.parse_table_attribute_pair(query_where[i]['Object'])
            table_pair = (ta1[0], ta2[0])
            if table_pair not in join_constraints:
                # Create empty list for key not already in join_constraints
                join_constraints[table_pair] = []
            join_constraints[table_pair].append(i)
    utils.test_print('map_join_constraints / join_constraints', join_constraints)
    return join_constraints


def map_value_constraints(query_where):
    """MAP_VALUE_CONSTRAINTS
        DESCRIPTION: 
        INPUT:
            - query_where: WHERE component of parsed SQL query
        OUTPUT: value_constraints: dict of lists; key of dict is the table that the list of
            requirements applies to; value of dict is a list of index numbers from the main
            WHERE query containing a constraint relevant to this table
    """
    value_constraints = {}
    
    for i in range(len(query_where)):
        if query_where[i]['Join'] == False:
            ta1 = utils.parse_table_attribute_pair(query_where[i]['Subject'])
            table1 = ta1[0]      # just parsing to make code more understandable
            if table1 not in value_constraints:
                # Create empty list for key not already in value_constraints
                value_constraints[table1] = []
            value_constraints[table1].append(i)
            
    utils.test_print('map_value_constraints / value_constraints', value_constraints)
    return value_constraints

def test_value_constraints(table, row, value_constraints, query_where, attribute_dict):
    """TEST_WHERE_CONSTRAINTS
        DESCRIPTION: 
        INPUT:
            - 
        OUTPUT: result: True if row passes WHERE constraints or if there are no constraints; 
            otherwise, False
    """
    result = True
    
    # loop through value_constraints that apply to this table
    # If there are none, that's OK: pass it.
    # Why pass it? If there is no value constraint, there should be a join constraint following.
    
    if table in value_constraints:
        constraint = 0
        constraints_list = []
        for i in value_constraints[table]:
            # even number constraints are results of individual value_constraint tests
            # odd numbers are boolean connectors between them
            
            constraint = constraint + 1
            if constraint > 1:
                # A connector is a boolean operator like AND. Only relevant if there is more than
                # one value constraint for a single table. Append to constraints_list in 
                # even-numbered slot.
                constraints_list.append(query_where[i]['Connector'])
        
            ta = utils.parse_table_attribute_pair(query_where[i]['Subject'])
            subj_index = utils.get_attribute_index(ta, attribute_dict)
            op = query_where[i]['Verb']
            obj = query_where[i]['Object']
            
            # Append result
            constraints_list.append(utils.eval_binary_comparison(row[subj_index], op, obj))
    
        # only need to do extra processing if there are multiple WHERE conditions to combine, i.e., len >=2
        #TODO: need to do some grouping when doing an OR compare on the same attribute
        result = constraints_list[0]
        if len(constraints_list) >= 2:
            for i in range(len(constraints_list)):
                if i % 2 == 0 and i < len(constraints_list) - 2:
                    a = result
                    op = constraints_list[i + 1]
                    b = constraints_list[i + 2]
                    result = utils.eval_binary_comparison(a, op, b)
    
    return result

def test_join_constraints(table1, row1, join_constraints, query_where, attribute_dict):
    """TEST_WHERE_CONSTRAINTS
        DESCRIPTION: 
        INPUT:
            - row: list of parsed values from a row in a csv file
            - where_constraints
            - attribute_dict
        OUTPUT: pass: True if row passes WHERE constraints or if there are no constraints; 
            otherwise, False
    """
    result = True
    # 0 - check join_constraints to see if there's even a table_1-table_x join
    # 0.1 - if so, make a list of table_x to check
    # 1 - for each row2 in each table_x:
    #       constraint.results.append(False)
    #       if row1 and row2 meet join_constraints: 
    #           join_results[i] = True; go to next constraint test
    # 2 - compare multiple join constraints against a single table1
    
    # 0 - Check for join constraints
    table2_list = []
    join_results = {}
    selected_join_rows = {}
    selected_join_rows[table1] = []
    
    for table_pair in join_constraints:
        if table_pair[0] == table1:
            if table_pair[1] not in table2_list:
                table2_list.append(table_pair[1])
                selected_join_rows[table_pair[1]] = []
    
    # 1 - Only need to do further processing if table1 shows up as the first in a pair of join_constraints
    #       (if it's the second, it will be handled later)
    # TODO: in query parsing, there should be some reordering of join constraints to limit re-reading files
    if len(table2_list) > 0:
        for t2 in range(len(table2_list)):
            if find_join_row(v1, a2, csv2) != '':
                join_results[t2] = True
    
    # 2 - Compare multiple join constraints on table 1, if there are any
    
    return result

def find_join_row(v1, a2, csv2):
    #TODO: no return value at the moment, perhaps should send something back to say yes/no found something
    found = False
    
    # don't join null values
    if v1 != '':
        utils.test_print('find_join_value / v1', v1)
        with open(csv2, newline = '', encoding = 'utf-8') as f2:
            i_row2 = 1
            r2 = csv.reader(f2)
            next(r2)        # Assumption: first row is header. Skip it.
        
            for row2 in r2:
                i_row2 = i_row2 + 1
                # Skip over blank rows, it's a killer
                if ''.join(row2).strip() == '':
                    continue
            
                # TODO: apply attribute constraints on t2
            
                v2 = row2[a2].strip()
                if v1 == v2:
                    utils.test_print('perform_query / v2', v2)
                    found = True
                    break
        f2.closed
    
    if found == False:
        i_row2 = 0
    
    return i_row2
    

def append_join_pair(i, table1, i_row1, table2, i_row2):
    
    filedir = utils.get_table_directory()
    filename = 'join_map_' + str(i) + '.csv'
    filepath = os.path.join(filedir, filename)
    
    writeline = [table1, i_row1, table2, i_row2]
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)
