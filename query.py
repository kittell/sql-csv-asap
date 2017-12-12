from utils import *
import csv
import os
import os.path
from parse import Query


def perform_query(q):
    """PERFORM_QUERY
    DESCRIPTION: This is where the query work starts. Take a parsed SQL query, apply it
        to the selected tables, and return the final results.
    INPUT: q: Query object with user input query information
    OUTPUT: final_select_results: list of resulting data values from query
    NOTES:
        - Assumption: SELECT and FROM are valid. They are the minimum to query.
    """
    
    # query format:
    #           q.WHERE[i]['Connector']
    #           q.WHERE[i]['Subject']
    #           q.WHERE[i]['Verb']
    #           q.WHERE[i]['Object']
    #           q.WHERE[i]['Join']

    final_select_results = []
    
    # wipe temp files that may exist - b/c we're appending temp results, not overwriting
    remove_temp_files()
    
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
        
    for table1 in q.query_table_list:
    
        if len(q.join_constraints) == 0:
            # CASE 1: NO JOINS
            # Just apply value_constraints to single table
            csv1 = table_to_csv_fullpath(table1)
            with open(csv1, newline = '', encoding = 'utf-8') as f1:
                test_print('perform_query / open(csv1)', csv1)
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
                    pass_value_constraints = test_value_constraints(table1, row1, q)
                    if pass_value_constraints == True:
                        append_filtered_row(table1, row1, i_row1)
                        int_select_results = project_row(int_select_results, row1, table1, q)
                        print(int_select_results)
            f1.closed

        else:
            # CASE 2: JOINS
            # Test table against value_constraints
            # Then test table against join_constraints. The join_constraints test will
            #   also include value_constraint tests against the second table.
            for table_pair in q.join_constraints:
                if table1 == table_pair[0]:
                    csv1 = table_to_csv_fullpath(table1)
                    with open(csv1, newline = '', encoding = 'utf-8') as f1:
                        test_print('perform_query / open(csv1)', csv1)
                        r1 = csv.reader(f1)
                        i_row1 = 1
                        next(r1)            # Assumption: first row is header. Skip it.
                        for row1 in r1:
                            i_row1 = i_row1 + 1

                            # Skip over blank rows
                            if ''.join(row1).strip() == '':
                                continue

                            # test row1 against value_constraints[table1]
                            pass_value_constraints = test_value_constraints(table1, row1, q)
                            if pass_value_constraints == True:
                                pass_join_constraints = test_join_constraints(table1, row1, i_row1, q)
                                if pass_join_constraints == True:
                                    append_filtered_row(table1, row1, i_row1)
                    f1.closed
    
    # Further processing for case 2: gather results from temp files and combine them
    for table_pair in q.join_constraints:
        table1 = table_pair[0]
        table2 = table_pair[1]
        filtered_tables = {}
        filtered_tables[table1] = get_filtered_table_fullpath(table1)
        filtered_tables[table2] = get_filtered_table_fullpath(table2)

        temp_join_file = get_temp_join_fullpath(table1, table2)
        test_print('perform_query / temp_join_file', temp_join_file)
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
                                            int_select_results = project_row(int_select_results, row_filtered[1:len(row_filtered)], table_filtered, q)
                                            break
                                f_filtered.closed
                    
                    print(int_select_results)

            f_join.closed
    
    return final_select_results

def project_row(results, row, table, q):
    for i in range(len(q.SELECT)):
        # figure out which SELECT statements apply to this table
        ta = parse_table_attribute_pair(q.SELECT[i])
        if ta[0] == table:
            # figure out the index in the row that holds the desired attribute value
            a = get_attribute_index(ta, q.attribute_dict)
            results.append(row[a])

    return results


def test_value_constraints(table, row, q):
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
    
    if table in q.value_constraints:
        constraint = 0
        constraints_list = []
        for i in q.value_constraints[table]:
            # even number constraints are results of individual value_constraint tests
            # odd numbers are boolean connectors between them
            
            constraint = constraint + 1
            if constraint > 1:
                # A connector is a boolean operator like AND. Only relevant if there is more than
                # one value constraint for a single table. Append to constraints_list in 
                # even-numbered slot.
                constraints_list.append(q.WHERE[i]['Connector'])
        
            ta = parse_table_attribute_pair(q.WHERE[i]['Subject'])
            subj_index = get_attribute_index(ta, q.attribute_dict)
            op = q.WHERE[i]['Verb']
            obj = q.WHERE[i]['Object']
            
            # Append result
            constraints_list.append(eval_binary_comparison(row[subj_index], op, obj))
    
        # only need to do extra processing if there are multiple WHERE conditions to combine, i.e., len >=2
        #TODO: need to do some grouping when doing an OR compare on the same attribute
        result = constraints_list[0]
        if len(constraints_list) >= 2:
            for i in range(len(constraints_list)):
                if i % 2 == 0 and i < len(constraints_list) - 2:
                    a = result
                    op = constraints_list[i + 1]
                    b = constraints_list[i + 2]
                    result = eval_binary_comparison(a, op, b)
    
    return result

def test_join_constraints(table1, row1, i_row1, q):
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
    
    for table_pair in q.join_constraints:
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
        csv2 = table_to_csv_fullpath(table2)
        with open(csv2, newline = '', encoding = 'utf-8') as f2:
#            test_print('test_join_constraints / open(csv2)', csv2)
            i_row2 = 1
            r2 = csv.reader(f2)
            next(r2)
            for row2 in r2:
                i_row2 = i_row2 + 1
                # Skip over blank rows, it's a killer
                if ''.join(row2).strip() == '':
                    continue
                
                # First: apply value constraints to row. if it doesn't pass that, no need to join
                pass_value_constraints = test_value_constraints(table2, row2, q)
                if pass_value_constraints == True:
                    # Then: compare join constraints between table1 and table 2
                    constraint = 0
                    constraints_list = []
                    for i in q.join_constraints[table1, table2]:
                        
#                        test_print('test_join_constraints / join_constraints', q.join_constraints)
                        # found a pair of tables
                        # next, find the rows that meet the join constraints
                        
                        # This is for handling multiple join constraints
                        constraint = constraint + 1
                        if constraint > 1:
                            # A connector is a boolean operator like AND. Only relevant if there is more than
                            # one value constraint for a single table. Append to constraints_list in 
                            # even-numbered slot.
                            constraints_list.append(q.WHERE['Connector'])
        
                        join_results[table2] = False
                        ta1 = q.WHERE[i]['Subject']
                        a1 = get_attribute_index(ta1, q.attribute_dict)
                        subj = row1[a1]
                        op = q.WHERE[i]['Verb']
                        ta2 = q.WHERE[i]['Object']
                        obj_index = get_attribute_index(ta2, q.attribute_dict)
            
                        # Append result
                        constraints_list.append(eval_binary_comparison(subj, op, row2[obj_index]))
#                        test_print('test_join_constraints / constraints_list', constraints_list)
    
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
                                result = eval_binary_comparison(a, op, b)

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
        test_print('find_join_value / v1', v1)
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
                    test_print('perform_query / v2', v2)
                    found = True
                    break
        f2.closed
    
    if found == False:
        i_row2 = 0
    
    return i_row2
    

def append_join_pair(table1, i_row1, table2, i_row2):
    
    filedir = get_table_directory()
    filename = 'temp_join_' + table1 + '_' + table2 + '.csv'
    filepath = os.path.join(filedir, filename)
    
    writeline = [table1, i_row1, table2, i_row2]
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)


def append_filtered_row(table, row, i_row):
    filepath = get_filtered_table_fullpath(table)
    
    writeline = []
    writeline.append(i_row)
    writeline = writeline + row
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)
    