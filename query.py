from utils import *
import csv
import os
import os.path
from parse import Query
from index import *

# TODO: With indexing, decide here if there is a relevant index for this table
# value_constraints contains the item numbers of constraint in q.WHERE
# So: we're interested in q.WHERE[c]['Subject'], which will hold a table.attr pair.
# Search the available indexes to see if there are any for these table.attr.
# For now, just find one and break. Will deal w/ multiple later, maybe.

def value_constraint_has_index(q, table_name):
    # Loop through value_constraints, find a matching table.attr index file
    if table_name in q.value_constraints:
        for c in q.value_constraints[table_name]:
            ta = q.WHERE[c]['Subject']
            ta_split = parse_table_attribute_pair(ta)
            if table_name == ta_split[0]:
                # now see if there's an index corresponding to an attribute
                if exists_index_file_keyword(ta_split[0], ta_split[1]) == True:
                    return True
    
    return False

def join_constraint_has_index(q, table_name='', attr_name=''):
    # Loop through value_constraints, find a matching table.attr index file
    if exists_index_file_keyword(table_name, attr_name) == True:
        return True
    
    return False

def get_index_byte_list(q, table_name, attr_name = '', attr_value = ''):
    """
    DESCRIPTION: Return a list of byte position numbers from an index for table_name.
    INPUT: 
        - Query q:
        - string table_name:
        - string attr_name: Name of attribute to find an index for. Should be empty for
            value_constraint index in order to find constraints for all attributes in 
            table. Should have a value for finding index on joins because that will be
            the specific joining attribute to find an index for.
        - string attr_value: 
    OUTPUT: list index_byte_list: list containing byte positions in table_name that 
        correspond to index values.
    """
    # TODO: Limitation: only can handle one condition from WHERE. Should consider the effects
    #   of multiple conditions.
    # TODO: What happens if this returns an empty list? Should there be a different signal
    #   that is returned if the index value isn't found in the index?
    index_byte_list = []
    
    if attr_name == '' and attr_value == '':
        # Case 1: value_constraints. attr_name and attr_value are empty. Find all indexes
        #   for table_name.
        if table_name in q.value_constraints:
            for c in q.value_constraints[table_name]:
                ta = q.WHERE[c]['Subject']
                ta_split = parse_table_attribute_pair(ta)
                if table_name == ta_split[0]:
                    # now see if there's an index corresponding to an attribute
                    attr_name = ta_split[1]
    
                    if ta in q.index_list:
                        index = read_index_file_keyword(table_name, attr_name)
                        obj = q.WHERE[c]['Object']
                        op = q.WHERE[c]['Verb']

                        for attr_value in index:
                            if eval_binary_comparison(attr_value, op, obj) == True:
                                for i in index[attr_value]:
                                    try:
                                        index_byte_list.append(int(i))
                                    except:
                                        continue

                        # TODO: break after finding first one; could compare multiple here
                        break
                                                              
    else:
        # Case 2: join_constraints. attr_name and attr_value have values.
        if exists_index_file_keyword(table_name, attr_name) == True:
            index = read_index_file_keyword(table_name, attr_name)
        
            if attr_value in index:
                for i in index[attr_value]:
                    try:
                        index_byte_list.append(int(i))
                    except:
                        continue

#    test_print('get_index_byte_list / index_byte_list:', index_byte_list)
    return index_byte_list


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
        
        # Get index info for table1
        has_index1 = value_constraint_has_index(q, table1)
        index_byte_list1 = get_index_byte_list(q, table1)
        remaining_row_list1 = index_byte_list1
    
#        print('index_byte_list1:',index_byte_list1)
    
        if len(q.join_constraints) == 0:
            # CASE 1: NO JOINS
            # Just apply value_constraints to single table
            
            csv1 = table_to_csv_fullpath(table1)
                                                
            with open(csv1, 'rb') as f1:
                test_print('perform_query / open(csv1)', csv1)
                
                # If this table is indexed, use the index to go to specific bytes in the file
                # and read the line. Otherwise: scan the entire table.
                
                if has_index1 == True:
                    # Has an index? Index scan.
                    for b1 in index_byte_list1:
                        f1.seek(b1)
                        line = readline_like_csv(f1)[1]
                        for row1 in csv.reader([line]):
                            int_select_results = []
                            # test row1 against value_constraints[table1]
                            pass_value_constraints = test_value_constraints(table1, row1, q)
                            if pass_value_constraints == True:
                                int_select_results = project_row(int_select_results, row1, table1, q)
                            if len(int_select_results) > 0:
                                final_select_results.append(int_select_results)
                            
                else:
                    # No index? Table scan.
                    b1 = 0
                    while True:
                        f1.seek(b1)
                        (b_returned, line) = readline_like_csv(f1)
                        if not line:
                            break
                        # Assume the first row is the header. Skip it.
                        # Also skip if it's a blank row.
                        if b1 > 0 and line != '':
                            for row1 in csv.reader([line]):
                                int_select_results = []
                                # test row1 against value_constraints[table1]
                                pass_value_constraints = test_value_constraints(table1, row1, q)
                                if pass_value_constraints == True:
                                    int_select_results = project_row(int_select_results, row1, table1, q)
                                if len(int_select_results) > 0:
                                    final_select_results.append(int_select_results)

                        b1 = b_returned

        else:
            # CASE 2: JOINS
            # Test table against value_constraints
            # Then test table against join_constraints. The join_constraints test will
            #   also include value_constraint tests against the second table.
            for table_pair in q.join_constraints:
                if table1 == table_pair[0]:
                    csv1 = table_to_csv_fullpath(table1)
                    with open(csv1, newline = '', encoding = 'utf-8') as f1:
                        test_print('perform_query / join / open(csv1)', csv1)

                        # Hold results until the end, then write to temp file
                        int_filter_results = []

                        
                        if has_index1 == True:
                            # Has an index? Index scan.
                            for b1 in index_byte_list1:
                                f1.seek(b1)
                                line = readline_like_csv(f1)[1]
                                for row1 in csv.reader([line]):
                                    # test row1 against value_constraints[table1]
                                    pass_value_constraints = test_value_constraints(table1, row1, q)
                                    if pass_value_constraints == True:
                                        # test row1 against join_constraints
                                        pass_join_constraints = test_join_constraints(table1, row1, b1, q)
                                        if pass_join_constraints == True:
                                            int_filter_results.append([table1, row1, b1])
#                                            append_filtered_row(table1, row1, b1)

                        else:
                            # No index? Table scan.
                            b1 = 0
                            while True:
                                f1.seek(b1)
                                (b_returned, line) = readline_like_csv(f1)
                                if not line:
                                    break
                                # Assume the first row is the header. Skip it.
                                # Also skip if it's a blank row.
                                if b1 > 0 and line.strip() != '':
                                    for row1 in csv.reader([line]):                        
                                        int_select_results = []
                                        # test row1 against value_constraints[table1]
                                        pass_value_constraints = test_value_constraints(table1, row1, q)
                                        if pass_value_constraints == True:
                                            # test row1 against join_constraints
                                            pass_join_constraints = test_join_constraints(table1, row1, b1, q)
                                            if pass_join_constraints == True:
                                                int_filter_results.append([table1, row1, b1])
#                                                append_filtered_row(table1, row1, b1)

                                b1 = b_returned
                    # After index or table scan, write int_join_results to a temp file
                    write_all_filtered_rows(int_filter_results, table1)
                    
    # After joining (if there was joining), combine the final results
    if len(q.join_constraints) > 0:
        final_select_results = combine_final_join_results(q)

    # Sort the final results (if ORDER BY was included in query)
    final_select_results = sort_final_results(final_select_results, q)

    return final_select_results

def sort_final_results(final_select_results, q):
    return final_select_results

def combine_final_join_results(q):
    """COMBINE_FINAL_JOIN_RESULTS
        DESCRIPTION: 
        INPUT:
        OUTPUT: 
    """
    final_select_results = []
    
    # Further processing for case 2: gather results from temp files and combine them
    # TODO: Split this out into a separate method
    for table_pair in q.join_constraints:
        test_print('perform_query / rejoin:', table_pair)
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
                        # Columns 0, 2, 4, etc. have table names; 1, 3, etc. have row numbers (i_filtered)
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
                                            # a byte #, not part of results
                                            int_select_results = project_row(int_select_results, row_filtered[1:len(row_filtered)], table_filtered, q)
#                                            test_print('perform_query / int_select_results:', int_select_results)
                                            
                                            break
                                f_filtered.closed
                    if len(int_select_results) > 0:
                        final_select_results.append(int_select_results)
    
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
    """TEST_VALUE_CONSTRAINTS
        DESCRIPTION: Given a data row from a table, check that it meets the associated
            value constraints from the user query q. What is a value constraint? It is a
            component of the WHERE clause that only relies on a given value, not on a
            second table, e.g., table.attr = value; vs. table1.attr = table2.attr.
        INPUT:
            - string table: table name of data being tested
            - list row: data row from table
            - Query q: full user input query
        OUTPUT: result: True if row passes WHERE value constraints or if there are no constraints; 
            otherwise, False
    """
    result = True
    
    # loop through value_constraints that apply to this table
    # If there are none, that's OK: pass it.
    
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

def get_individual_join_constraint_results(q, table1, row1, table2, row2):
    """GET_INDIVIDUAL_JOIN_CONSTRAINT_RESULTS
        DESCRIPTION: For a given table1 and table2, find join constraints from the WHERE
            clause of the user query that involve table1 on left side and table 2 on right side.
        INPUT:
            - string table: table name of data being tested
            - list row: data row from table
            - Query q: full user input query
        OUTPUT: list constraints_list: 
    """
    constraint_results = []
    for i in q.join_constraints[table1, table2]:
        # Found a pair of tables in join_constraints. Because there may be multiple join
        # constraints for a table1-table2 pair, parse all join_constraints for this pair
        # into a single list built for comparison.

        # A connector is a boolean operator like AND that links multiple constraints.
        # The first one is always an empty string.
        constraint_results.append(q.WHERE[i]['Connector'])

        ta1 = q.WHERE[i]['Subject']
        a1 = get_attribute_index(ta1, q.attribute_dict)
        subj = row1[a1]
        op = q.WHERE[i]['Verb']
        ta2 = q.WHERE[i]['Object']
        obj_index = get_attribute_index(ta2, q.attribute_dict)

        # Append result
        constraint_results.append(eval_binary_comparison(subj, op, row2[obj_index]))
        
    return constraint_results

def compare_join_constraints(q, table1, row1, table2, row2):
    """COMPARE_JOIN_CONSTRAINTS
        DESCRIPTION: Handles the comparison of join constraints on a given table1-table2 pair.
            If there is only one join constraint, it returns the result. If there are more
            than one, it compares the results of each join constraint, based on the boolean
            operator that connects them
        INPUT:
            - string table: table name of data being tested
            - list row: data row from table
            - Query q: full user input query
        OUTPUT: list constraints_list: 
    """
    # Compare multiple join constraints on table1-table2, if there are any
    # only need to do extra processing if there are multiple WHERE conditions to combine.
    #TODO: need to do some grouping when doing an OR compare on the same attribute
    
    constraint_results = get_individual_join_constraint_results(q, table1, row1, table2, row2)

    if len(constraint_results) == 0:
        # If no constraints, result is pass/True (as in, it doesn't fail anything.)
        result = True
    elif len(constraint_results) < 3:
        # If the size is less than 3, there is only one constraint (and an empty connector), 
        # so don't have to do any comparisons to others
        result = constraint_results[1]
    else:
        # Need to compare multiple constraint results
        result = constraint_results[1]
        for i in range(2, len(constraint_results)):
            if i % 2 == 0:
                # Take the constraint_results two-at-a-time from the list. The first is 
                # the connector (e.g., AND); the second is the individual constraint result.
                # Starting with i = 2 b/c there is nothing before the first constraint_results
                # to compare it to.
                a = result                      # previous result term
                op = constraint_results[i]      # op will be a boolean operator
                b = constraint_results[i + 1]   # next result term
                result = eval_binary_comparison(a, op, b)
    return result

def test_join_constraints(table1, row1, b1, q):
    """TEST_JOIN_CONSTRAINTS
        DESCRIPTION: A data row passed to test_join_constraints has already been tested
            against any value constraints (i.e., non-join constraints) from the WHERE clause
            in the query. After that, it is passed to test_join_constraints to see if it
            meets any join constraints from the query.
        INPUT:
            - table1: table containing the data row of the front half of the query
            - row1: data row from the front half of the query; has already passed value_constraints
            - b1: byte position of the start of row1 in table1
            - q: user query object
        OUTPUT: boolean persistent_result: True if row1 passes its join constraints
            (if there are no join constraints, also True); otherwise, False
    """
    result = True
    persistent_result = False
    
    # 0 - check join_constraints to see if there's even a table_1-table_x join in WHERE
    # 0.1 - if so, make a list of table2's to check
    # 1 - for each table2:
    #   ...
    # 2 - compare multiple join constraints against a single table1
    
    # 0 - Check for join constraints
    # Determine which table2's need to be opened and tested
    table2_list = []
    for table_pair in q.join_constraints:
        if table_pair[0] == table1:
            if table_pair[1] not in table2_list:
                table2_list.append(table_pair[1])
    
    # Exit here if there are no join constraints to check, i.e., not table 2 for this table1
    if len(table2_list) == 0:
        return True
    
    # 1 - Only need to do further processing if table1 shows up as the first in a pair of join_constraints
    #       (if it's the second, it will be handled later)
    
    # loop through available combos of table2 (of a table1-table2 pair)
    # loop through the where constraints on that pair
    for table2 in table2_list:

        for table_pair in q.join_constraints:
            if table_pair[0] == table1 and table_pair[1] == table2:
                # Therefore: A join constraint exists on this table1-table2 pair, e.g.,
                #   WHERE table1.attr_name1 = table2.attr_name2
                
                # Get attribute name from right side of join equation. There may be 
                # more than one on this pair. Try to find an attribute name that has
                # an associated index; if not, take the attribute name of the first pair.
                constraint_list = q.join_constraints[table_pair]
                i_constraint = 0
                for i in constraint_list:
                    ta2 = q.WHERE[i]['Object']
                    if ta2 in q.index_list:
                        # Found this table.attr pair in the index list
                        i_constraint = i
                        break
                    
                ta1 = q.WHERE[i_constraint]['Subject']
                ta2 = q.WHERE[i_constraint]['Object']
                ta_split1 = parse_table_attribute_pair(ta1)
                ta_split2 = parse_table_attribute_pair(ta2)
                attr_name2 = ta_split2[1]
                    
                # INDEXING: Get index info for table2.attr_name2
                has_index2 = join_constraint_has_index(q, table2, attr_name2)

                # Get the attr1 value for this specific WHERE join constraint
                attr_index1 = get_attribute_index(ta1, q.attribute_dict)
                attr_value1 = row1[attr_index1]
        
                index_byte_list2 = get_index_byte_list(q, table2, attr_name2, attr_value1)
        
                # walk through table 2
                csv2 = table_to_csv_fullpath(table2)
                with open(csv2, newline = '', encoding = 'utf-8') as f2:
#                    test_print('test_join_constraints / open(csv2)', csv2)
            
                    if has_index2 == True:
                        # Has an index? Index scan.
                        for b2 in index_byte_list2:
                            f2.seek(b2)
                            line = readline_like_csv(f2)[1]
                            for row2 in csv.reader([line]):
                                # First: apply value constraints to row. if it doesn't pass that, no need to join
                                pass_value_constraints = test_value_constraints(table2, row2, q)
                                if pass_value_constraints == True:
                                    # Then: compare join constraints between table1 and table2
                                    result = compare_join_constraints(q, table1, row1, table2, row2)

                                    if result == True:
                                        persistent_result = True
#                                        append_join_pair(table1, b1, table2, b2)
#                                        append_filtered_row(table2, row2, b2)
                                        

                    else:
                        # No index? Table scan.
                        b2 = 0
                        while True:
                            f2.seek(b2)
                            (b_returned, line) = readline_like_csv(f2)
                            if not line:
                                break
                            # Assume the first row is the header. Skip it.
                            # Also skip if it's a blank row.
                            if b2 > 0 and line.strip() != '':
                                for row2 in csv.reader([line]):
                                    int_select_results = []
                                    # test row1 against value_constraints[table1]
                                    pass_value_constraints = test_value_constraints(table2, row2, q)
                                    if pass_value_constraints == True:
                                        # Then: compare join constraints between table1 and table2
                                        result = compare_join_constraints(q, table1, row1, table2, row2)

                                        if result == True:
                                            persistent_result = True
                                            append_join_pair(table1, b1, table2, b2)
                                            append_filtered_row(table2, row2, b2)
                            b2 = b_returned
    
    return persistent_result


def append_join_pair(table1, b1, table2, b2):
    filepath = get_temp_join_fullpath(table1, table2)
    
    writeline = [table1, b1, table2, b2]
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)

def write_all_join_pairs(list_of_lists, table1, table2):
    filepath = get_temp_join_fullpath(table1, table2)
    with open(filepath, 'w', newline='') as f:
        for line in list_of_lists:
            w = csv.writer(f)
            w.writerow(line)
    

def append_filtered_row(table, row, b):
    filepath = get_filtered_table_fullpath(table)
    
    writeline = []
    writeline.append(b)
    writeline = writeline + row
    
    with open(filepath, 'a', newline = '') as f:
        w = csv.writer(f)
        w.writerow(writeline)

def write_all_filtered_rows(list_of_lists, table):
    # Basically it's just a list of things to append_filtered_row on
    
    filepath = get_filtered_table_fullpath(table)
    with open(filepath, 'w', newline='') as f:
        for line in list_of_lists:
            w = csv.writer(f)
            w.writerow(line)
        