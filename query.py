import utils
import csv
import os
import os.path

def build_table_list(query):
    """BUILD_TABLE_LIST
    DESCRIPTION: List all the tables stated in the SELECT and WHERE clauses
    INPUT: query: the parsed user query
    OUTPUT: table_list: list containing names of all tables in query
    """
    table_list = []
    # 1 - pull tables from WHERE
    for i in range(len(query['WHERE'])):
        ta1 = utils.parse_table_attribute_pair(query['WHERE'][i]['Subject'])
        if ta1[0] not in table_list:
            table_list.append(ta1[0])
        
        if query['WHERE'][i]['Join'] == True:
            ta2 = utils.parse_table_attribute_pair(query['WHERE'][i]['Object'])
            if ta2[0] not in table_list:
                table_list.append(ta2[0])
        
    # 2 - pull tables from SELECT
    for i in range(len(query['SELECT'])):
        ta1 = utils.parse_table_attribute_pair(query['SELECT'][i])
        if ta1[0] not in table_list:
            table_list.append(ta1[0])
        
    return table_list

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
    
    # Get list of tables from WHERE query.
    table_list = build_table_list(query)
    print(table_list)

    # Build attribute dict. Used to associate attribute names (row 1) with their data.
    attribute_dict = {}
    for table in table_list:
        csv_fullpath = utils.table_to_csv_fullpath(table)
        attribute_dict = utils.get_attribute_dict(attribute_dict, csv_fullpath)
        
    # Determine which WHERE clauses are joins or attribute constraints
    query_where = query['WHERE']
    join_constraints = map_join_constraints(query_where)
    value_constraints = map_value_constraints(query_where)
    
    # wipe temp files that may exist - b/c we're appending temp results, not overwriting
    utils.remove_temp_files()
    
    # Case 1: no joins. Simply walk through single table, applying all value constraints
    # Case 2: joins.
        # - Walk through value constraints of first table, table1. For rows that pass, append the
        #       row to a temp file: temp_filtered_table1.csv
        # - Then, apply join_constraints that involve table1. For the row # that just
        #       passed the value_constraints, walk through table2
        #   * Apply any value_constraints to table2 rows. If the row passes its value_constraints,
        #       apply the relevant join_constraints. If the row passes join_ and value_constraints:
        #       - Append the join details (table1, #_row1, table2, #_row2) to a temp file:
        #           temp_join_table1_table2.csv
        #       - Append the values of row2 to a temp file: temp_filtered_table2.csv
        # - After applying all constraints to all rows, gather the data from the temp files.
        #   * Walk through the temp_join files, using table1, row1 to get the relevant 
        #       information from the temp_filtered_table1.csv file, and the same with
        #       table2, row2. Merge the information together into a single line and print it.
        
    for table1 in table_list:
    
        if len(join_constraints) == 0:
            # CASE 1: NO JOINS
            # Just apply value_constraints to single table
            csv1 = utils.table_to_csv_fullpath(table1)
            with open(csv1, newline = '', encoding = 'utf-8') as f1:
                utils.test_print('perform_query / open(csv1)', csv1)
                r1 = csv.reader(f1)
                i_row1 = 1
                next(r1)            # Assumption: first row is header. Skip it.
                for row1 in r1:
                    i_row1 = i_row1 + 1
                    int_select_results = []
                    
                    # Skip over blank rows
                    if ''.join(row1).strip() == '':
                        continue
                    
                    # test row1 against value_constraints[table1]
                    pass_value_constraints = test_value_constraints(table1, row1, value_constraints, query['WHERE'], attribute_dict)
                    if pass_value_constraints == True:
                        append_filtered_row(table1, row1, i_row1)
                        int_select_results = project_row(int_select_results, row1, table1, query['SELECT'], attribute_dict)
                        print(int_select_results)
            f1.closed

        else:
            # CASE 2: JOINS
            # Test table against value_constraints
            # Then test table against join_constraints. The join_constraints test will
            #   also include value_constraint tests against the second table.
            for table_pair in join_constraints:
                if table1 == table_pair[0]:
                    csv1 = utils.table_to_csv_fullpath(table1)
                    with open(csv1, newline = '', encoding = 'utf-8') as f1:
                        utils.test_print('perform_query / open(csv1)', csv1)
                        r1 = csv.reader(f1)
                        i_row1 = 1
                        next(r1)            # Assumption: first row is header. Skip it.
                        for row1 in r1:
                            i_row1 = i_row1 + 1

                            # Skip over blank rows
                            if ''.join(row1).strip() == '':
                                continue

                            # test row1 against value_constraints[table1]
                            pass_value_constraints = test_value_constraints(table1, row1, value_constraints, query['WHERE'], attribute_dict)
                            if pass_value_constraints == True:
                                pass_join_constraints = test_join_constraints(table1, row1, i_row1, join_constraints, query_where, attribute_dict, value_constraints)
                                if pass_join_constraints == True:
                                    append_filtered_row(table1, row1, i_row1)
                    f1.closed
    
    # Further processing for case 2: gather results from temp files and combine them
    for table_pair in join_constraints:
        table1 = table_pair[0]
        table2 = table_pair[1]
        filtered_tables = {}
        filtered_tables[table1] = utils.get_filtered_table_fullpath(table1)
        filtered_tables[table2] = utils.get_filtered_table_fullpath(table2)

        temp_join_file = utils.get_temp_join_fullpath(table1, table2)
        utils.test_print('perform_query / temp_join_file', temp_join_file)
        if os.path.exists(temp_join_file) == True:
            # open temp_join file
            with open(temp_join_file, newline = '', encoding = 'utf-8') as f_join:
                # Walk through temp_join_file. This contains information about joined
                # tables: (table1, i_row1, table2, i_row2)
                # Find i_row1 and i_row2 in their corresponding temp_filtered files
                r_join = csv.reader(f_join)
                for row_join in r_join:
                    int_select_results = []
                    for col in range(len(row_join)):
                        # Next: go to filtered_table files to retrieve data
                        # Columns 0, 2, 4, etc. have table namess; 1, 3, etc. have row numbers (i_filtered)
                        if col % 2 == 0:
                            if os.path.exists(filtered_tables[row_join[col]]):
                                with open(filtered_tables[row_join[col]], newline = '', encoding = 'utf-8') as f_filtered:
                                    table_filtered = row_join[col]
                                    i_filtered = row_join[col + 1]
                                    r_filtered = csv.reader(f_filtered)
                                    for row_filtered in r_filtered:
                                        if row_filtered[0] == i_filtered:
                                            # This is the data you want; project it into final list
                                            # Note: leaving out first entry in row_filtered: it is
                                            # a row #, not part of results
                                            int_select_results = project_row(int_select_results, row_filtered[1:len(row_filtered)], table_filtered, query['SELECT'], attribute_dict)
                                            break
                                f_filtered.closed
                    
                    print(int_select_results)

            f_join.closed
    
    return final_select_results

def project_row(results, row, table, query_select, attribute_dict):
    for i in range(len(query_select)):
        # figure out which SELECT statements apply to this table
        ta = utils.parse_table_attribute_pair(query_select[i])
        if ta[0] == table:
            # figure out the index in the row that holds the desired attribute value
            a = utils.get_attribute_index(ta, attribute_dict)
            results.append(row[a])

    return results

def map_join_constraints(query_where):
    """MAP_JOIN_CONSTRAINTS
        DESCRIPTION: 
        INPUT:
            - query_where: WHERE component of parsed SQL query
        OUTPUT: join_constraints: dict of list; key of dict is a tuple of (table1, table2);
            value of dict is a list of index numbers from main WHERE query containing 
            constraint relevant to this table-table pair
            e.g., join_constraints[(table1, table2)] = 0
                  join_constraints[(table2, table3)] = 1
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

def test_join_constraints(table1, row1, i_row1, join_constraints, query_where, attribute_dict, value_constraints):
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
    persistent_result = False
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
    
    for table_pair in join_constraints:
        if table_pair[0] == table1:
            if table_pair[1] not in table2_list:
                table2_list.append(table_pair[1])
    
    # 1 - Only need to do further processing if table1 shows up as the first in a pair of join_constraints
    #       (if it's the second, it will be handled later)
    # TODO: in query parsing, there should be some reordering of join constraints to limit re-reading files
    
    # loop through available combos of table2 (of a table1-table2 pair)
    # loop through the where constraints on that pair
    for table2 in table2_list:
        # walk through table 2
        csv2 = utils.table_to_csv_fullpath(table2)
        with open(csv2, newline = '', encoding = 'utf-8') as f2:
#            utils.test_print('test_join_constraints / open(csv2)', csv2)
            i_row2 = 1
            r2 = csv.reader(f2)
            next(r2)
            for row2 in r2:
                i_row2 = i_row2 + 1
                # Skip over blank rows, it's a killer
                if ''.join(row2).strip() == '':
                    continue
                
                # First: apply value constraints to row. if it doesn't pass that, no need to join
                pass_value_constraints = test_value_constraints(table2, row2, value_constraints, query_where, attribute_dict)
                if pass_value_constraints == True:
                    # Then: compare join constraints between table1 and table 2
                    constraint = 0
                    constraints_list = []
                    for i in join_constraints[table1, table2]:
                        
#                        utils.test_print('test_join_constraints / join_constraints', join_constraints)
                        # found a pair of tables
                        # next, find the rows that meet the join constraints
                        
                        # This is for handling multiple join constraints
                        constraint = constraint + 1
                        if constraint > 1:
                            # A connector is a boolean operator like AND. Only relevant if there is more than
                            # one value constraint for a single table. Append to constraints_list in 
                            # even-numbered slot.
                            constraints_list.append(query_where[i]['Connector'])
        
                        join_results[table2] = False
                        ta1 = query_where[i]['Subject']
                        a1 = utils.get_attribute_index(ta1, attribute_dict)
                        subj = row1[a1]
                        op = query_where[i]['Verb']
                        ta2 = query_where[i]['Object']
                        obj_index = utils.get_attribute_index(ta2, attribute_dict)
            
                        # Append result
                        constraints_list.append(utils.eval_binary_comparison(subj, op, row2[obj_index]))
#                        utils.test_print('test_join_constraints / constraints_list', constraints_list)
    
                    # 2 - Compare multiple join constraints on table 1, if there are any
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

                    if result == True:
                        persistent_result = True
                        append_join_pair(table1, i_row1, table2, i_row2)
                        append_filtered_row(table2, row2, i_row2)

        f2.closed
    
    return persistent_result

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
    

def append_join_pair(table1, i_row1, table2, i_row2):
    
    filedir = utils.get_table_directory()
    filename = 'temp_join_' + table1 + '_' + table2 + '.csv'
    filepath = os.path.join(filedir, filename)
    
    writeline = [table1, i_row1, table2, i_row2]
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)


def append_filtered_row(table, row, i_row):
    filepath = utils.get_filtered_table_fullpath(table)
    
    writeline = []
    writeline.append(i_row)
    writeline = writeline + row
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)
    