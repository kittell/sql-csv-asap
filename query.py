from utils import *
import csv
import os
import os.path
from parse import Query
from index import *

def perform_query(Q, I):
    """PERFORM_QUERY
    DESCRIPTION: This is where the query work starts. Take a parsed SQL query, apply it
        to the selected tables, and return the final results.
    INPUT: Q: Query object with user input query information
    OUTPUT: final_select_results: list of resulting data values from query
    NOTES:
        - Assumption: SELECT and FROM are valid. They are the minimum to query.
    """

    R = ResultsManager()
    
    # For each table, get the results filtered by value_constraints in the WHERE clause
    filter_value_constraints(Q, I, R)
    
    # For each table pair, get the table1-table2 pairs resulting from join_constraints in WHERE clause
    filter_join_constraints(Q, I, R)
    
    test_print('\nR.filtered_headers_dict',R.filtered_headers_dict)
    test_print('\nR.filtered_results_dict',R.filtered_results_dict)
    test_print('\nR.join_dict',R.join_dict)
    
    # Combine the results
    combine_final_results(Q, R)
    
    # Handle ORDER BY
    order_final_results(Q, R)
    
    # Handle SELECT DISTINCT
    distinct_final_results(Q, R)
    
    return R.final_results

def filter_join_constraints(Q, I, R):
    """
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    join_results_dict = {}
    #join_results_dict[table1, table2] = [(b1, b2)]
    
    # If any tables didn't have a filter, capture data from the table2 rows that pass the join_constraints
    # into new_filtered_dict. Apply to R.filtered_results_dict. Data will be used at the
    # end of the query to project the final results.
    new_filtered_dict = {}
    new_filtered_dict_headers = {}
    
    # Build list of tables to check
    table1_list = []
    for table_pair in Q.join_constraints:
        if table_pair not in R.join_dict:
            R.join_dict[table_pair] = []
        (table1, table2) = table_pair
        if table1 not in table1_list:
            table1_list.append(table1)
    
    for table1 in table1_list:
    
        # Get (a) filtered byte_list or (b) index byte_list or (c) need to table scan
        has_index1 = value_constraint_has_index(Q, I, table1)
        has_filter1 = False
        if table1 in R.filtered_results_dict:
            if R.filtered_results_dict[table1] != None:
                # So: Table1 was filtered, returned no results (if None, not filtered)
                has_filter1 = True
        else:
            if table1 not in new_filtered_dict:
                new_filtered_dict[table1] = {}
                new_filtered_dict_headers[table1] = project_row(Q, table1, 'header')
                
        
        byte_list1 = None
        if has_filter1 == True:
            # Case a: Table has been filtered, use byte positions returned from filter.
            byte_list1 = []
            for b in R.filtered_results_dict[table1]:
                byte_list1.append(b)
        elif has_index1 == True:
            # Case b: No filter, but there is an index for this table. Use byte positions from index.
            byte_list1 = []
            byte_list1 = get_index_byte_list(Q, I, table1)
        # Case c: If byte_list1 is still None at this point, need to do a table scan on table1
        
        # Loop over table1
        csv_fullpath1 = table_to_csv_fullpath(table1)
        f1 = open(csv_fullpath1, 'rb')
        b1 = 0
        i1 = 0

        while True:
            # Determine the next byte position b1 to go to in f1
            if byte_list1 != None:
                if len(byte_list1) == 0:
                    # This is the case where table1 was filtered, and all results were filtered out.
                    # So: Stop scanning anything with table1 as first table in pair, there will be no joins.
                    break
                else:
                    # Retrieve next byte position from byte list, if filter or index is available
                    b1 = byte_list1[i1]
                    i1 += 1

            else:
                # (c) table scan
                pass
        
            # Return the line from position b1 in the file
            f1.seek(b1)
            (b1_returned, line1) = readline_like_csv(f1)
        
            # Line will read empty at end of file
            if not line1:
                break
            
            # Skip first row (header) and blank rows
            if b1 > 0 and line1 != '':
                for row1 in csv.reader([line1]):
                    # Search for various table1-table 2 join constraints
                    for table_pair in Q.join_constraints:
                        # Only check constraints where table1 is the first table
                        if table_pair[0] != table1:
                            continue
                        
                        # Open table2 for reading
                        table2 = table_pair[1]
                        
                        # Check filter before entering join_constraints[table_pair] loop. If it is an empty dict now,
                        # it will be replaced with values as the join constraints are evaluated.
                        # i.e., it would indicate a filter on table2 exists when it doesn't.
                        has_filter2 = False
                        if table2 in R.filtered_results_dict:
                            if bool(R.filtered_results_dict[table2]):
                                # This will return false if R.filtered_results_dict[table2] is an empty dict
                                # So: Table1 was filtered, returned no results (if None, not filtered)
                                has_filter2 = True

                        else:
                            if table2 not in new_filtered_dict:
                                new_filtered_dict[table2] = {}
                                new_filtered_dict_headers[table2] = project_row(Q, table2, 'header')
                        
                        csv_fullpath2 = table_to_csv_fullpath(table2)
                        f2 = open(csv_fullpath2, 'rb')
                        
                        for c2 in Q.join_constraints[table_pair]:
                            attr_name1 = parse_table_attribute_pair(Q.WHERE[c2]['Subject'])[1]
                            attr_index1 = get_attribute_index(combine_table_attribute_pair(table1, attr_name1), Q.attribute_dict)
                            attr_value1 = row1[attr_index1]
                            attr_name2 = parse_table_attribute_pair(Q.WHERE[c2]['Object'])[1]
                            has_index2 = join_constraint_has_index(Q, table2, attr_name2)
                            
                            # Determine whether: (a) filtered byte_list or (b) index byte_list or (c) need to table scan
                            byte_list2 = None
                            if has_filter2 == True:
                                # Case a: Table has been filtered, use byte positions returned from filter.
                                byte_list2 = []
                                for b in R.filtered_results_dict[table2]:
                                    byte_list2.append(b)

                            elif has_index2 == True:
                                # Case b: No filter, but there is an index for this table.
                                # Use byte positions from index for attr_name2 and attr_value1 (yes, attr_value2 is based on attr_value1)
                                byte_list2 = get_index_byte_list(Q, I, table2, attr_name2, attr_value1)

                            # Case c: If byte_list2 is still None at this point, need to do a table scan on table2
                            
                            # Loop over table2
                            b2 = 0
                            i2 = 0
                            int_result = []
                            while True:
                                if b2 == 0:
                                    pass
                                # Determine the next byte position b2 to go to in f2
                                if byte_list2 != None:
                                    if len(byte_list2) == 0:
                                        # This is the case where table2 was filtered, and all results were filtered out.
                                        # So: Stop scanning anything with table1 as first table in pair, there will be no joins.
                                        break
                                    else:
                                        # Retrieve next byte position from byte list, if filter or index is available
                                        b2 = byte_list2[i2]
                                        i2 += 1
                                        
                                else:
                                    # (c) table scan
                                    pass
                                                                
                                # Return the line from position b2 in the file
                                f2.seek(b2)
                                (b2_returned, line2) = readline_like_csv(f2)
                                
                                # Line will read empty at end of file for table scan
                                if not line2:
                                    break
                    
                                # Skip first row (header) and blank rows
                                if b2 > 0 and line2 != '':
                                    for row2 in csv.reader([line2]):
                                        if compare_join_constraints(Q, table1, row1, table2, row2) == True:
                                            # row1-row2 passes join constraints. Capture the results.
                                            R.join_dict[table_pair].append((b1, b2))
                                            
                                            # Add to filter_dict[table2] if there was no filter before starting.
                                            if table2 in new_filtered_dict:
                                                if b2 not in new_filtered_dict[table2]:
                                                    new_filtered_dict[table2][b2] = project_row(Q, table2, '', row2)
                                            if table1 in new_filtered_dict:
                                                if b1 not in new_filtered_dict[table1]:
                                                    new_filtered_dict[table1][b1] = project_row(Q, table1, '', row1)

                                # Check if byte_list2 has been exhausted -- if so, exit
                                if byte_list2 != None:
                                    if i2 == len(byte_list2):
                                        break
                                b2 = b2_returned
                        f2.close()

            # Check if byte_list1 has been exhausted -- if so, exit
            if byte_list1 != None:
                if i1 == len(byte_list1):
                    break
            b1 = b1_returned
    
        f1.close()
    
    # Feed new_filtered_dict into R.filtered_results_dict
    for table in new_filtered_dict:
        if table not in R.filtered_results_dict:
            R.filtered_results_dict[table] = {}
        if table not in R.filtered_headers_dict:
            R.filtered_headers_dict[table] = {}
        R.filtered_results_dict[table] = new_filtered_dict[table]
        R.filtered_headers_dict[table] = new_filtered_dict_headers[table]
    
    
def filter_value_constraints(Q, I, R):
    """
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    # TODO: Need to handle case for no WHERE constraints...
    #filtered_results_dict[table1][b] = [data]
    
    for table in Q.table_list:
        if table in Q.value_constraints or len(Q.value_constraints) == 0:
            test_print('filter_value_constraints',table)
            # Set up dict to hold results
            if table not in R.filtered_results_dict:
                R.filtered_results_dict[table] = {}
                
                # flag: If you get to the end of this filter, and there are no results
                flag_no_results = True
            
            # Calculate headers
            R.filtered_headers_dict[table] = project_row(Q, table, 'header')
            
            # Get index byte_list for this table, if it exists.
            # TODO: replace this w/ IndexManager info
            has_index = value_constraint_has_index(Q, I, table)
            
            byte_list = []
            if has_index == True:
                byte_list = get_index_byte_list(Q, I, table)
            
            # Open the table file for reading
            csv_fullpath = table_to_csv_fullpath(table)
            f = open(csv_fullpath, 'rb')
            b = 0
            i = 0
            while True:
                if has_index == True:
                    b = byte_list[i]
                    i += 1
                
                # Return the line from position b in the file
                f.seek(b)
                (b_returned, line) = readline_like_csv(f)
                
                # Line will read empty at end of file for table scan
                if not line:
                    break
                    
                # Skip first row (header) and blank rows
                if b > 0 and line != '':
                    for row in csv.reader([line]):
                        int_select_results = []
                        # Test row against value_constraints
                        if test_value_constraints(table, row, Q) == True:
                            int_select_results = project_row(Q, table, '', row)
                        if len(int_select_results) > 0:
                            R.filtered_results_dict[table][b] = int_select_results
                            flag_no_results = False
                
                # Index will be out of range at end of index scan
                if has_index == True:
                    if i == len(byte_list):
                        break
                
                b = b_returned
            f.close()
            
            # If zero results are returned, that means none met the criteria
            # To signal this, set the return value to None
            if flag_no_results == True:
                R.filtered_results_dict[table] = None
    

def order_final_results(Q, R):
    # TODO: Sort on Q.ORDERBY
    pass
    
def distinct_final_results(Q, R):
    if Q.distinct == True:
        temp_list = []
        for row in R.final_results:
            if row not in temp_list:
                temp_list.append(row)
        R.final_results = temp_list

def combine_final_results(Q, R):
    """COMBINE_FINAL_RESULTS
        DESCRIPTION: 
        INPUT:
        OUTPUT: 
    """
    #R.filtered_results_dict[table][b] = [data]
    #R.filtered_headers_dict[table] = [headers]
    #R.join_dict[table1, table2] = [(b1, b2), ...]
    
    combined_results = []
    
    # First: add headers to combined_results
    # TODO: If handling AS alias, use that instead
    header = []
    for i in range(len(Q.SELECT)):
        table_attr = Q.SELECT[i]
        header.append(table_attr)
    combined_results.append(header)
    
    # CASE 1: NO JOIN.
    if len(R.join_dict) == 0:
        for table in R.filtered_results_dict:
            for b in R.filtered_results_dict[table]:
                row_results = []
                for i in range(len(Q.SELECT)):
                    table_attr = Q.SELECT[i]
                    # From R.filtered_headers_dict, which position in R.filtered_results_dict
                    # corresponds to this SELECT table_attr?
                    (table, attr_name) = parse_table_attribute_pair(table_attr)
                    for j in range(len(R.filtered_headers_dict[table])):
                        if R.filtered_headers_dict[table][j] == table_attr:
                            # Found the table_attr location in headers. Now get the data from R.filtered_results_dict.
                            row_results.append(R.filtered_results_dict[table][b][j])
                            break
                if len(row_results) > 0:
                    combined_results.append(row_results)
    
    # CASE 2: JOIN. Combine join constraints with filtered data.
    else:
        # General case: For each (table1,table2) pair in R.join_dict:
        #   For each (b1,b2) pair in R.join_dict[(table1,table2)]
        #       Search out matching results in other R.join_dict[(table,table)] pairs
        # e.g., for a three table join, need to match b2 of R.join_dict[(table1,table2)] = (b1,b2)
        # with b2 of R.join_dict[(table2,table3)] = (b2,b3)
        # End result will be list of [b1,b2,b3] from which to gather data from R.filtered_results_dict
                
        final_join_results = []
        # join_table_list is a map to table headers
        join_table_list = []
        for (table1, table2) in Q.join_constraints:
            if table1 not in join_table_list:
                join_table_list.append(table1)
            if table2 not in join_table_list:
                join_table_list.append(table2)
                
        n_join_tables = len(join_table_list)
        joined_tables = []
        for (table1, table2) in R.join_dict:
            # Get position of table1, table2 in join_table_list
            for t in range(len(join_table_list)):
                if table1 == join_table_list[t]:
                    pos_b1 = t
                elif table2 == join_table_list[t]:
                    pos_b2 = t
        
            # Case: final_join_results is empty, add all table1:b1, table2:b2 for this (table1, table2)
            # Also: it's the full solution for a two-table join
            join_results_empty = False
            if len(final_join_results) == 0:
                join_results_empty = True

            new_final_join_results = []
            for (b1, b2) in R.join_dict[(table1, table2)]:
                # Initialize int_result to length of number of tables to join
                if join_results_empty == True:
                    # don't think, just add
                    new_row = [None] * n_join_tables
                    new_row[pos_b1] = b1
                    new_row[pos_b2] = b2
                    new_final_join_results.append(new_row)

                else:
                    
                    for row in range(len(final_join_results)):
                        # Match b1 in row
                        #   If position is None, add b2 to it
                        #   Else if position is filled with a value different than b2:
                        #       add a new row with b1 and b2
                        # Match b2...
                        
                        if final_join_results[row][pos_b1] == b1:
                            if final_join_results[row][pos_b2] == None:
                                # Add b2 to an empty place
                                new_row = final_join_results[row]
                                new_row[pos_b2] = b2
                                new_final_join_results.append(new_row)
                            elif final_join_results[row][pos_b2] != b2:
                                # Multiple b1 matches, add a new row
                                new_final_join_results.append(final_join_results[row])
                                new_row = final_join_results[row]
                                new_row[pos_b1] = b1
                                new_row[pos_b2] = b2
                                new_final_join_results.append(new_row)
                            else:
                                new_final_join_results.append(final_join_results[row])
                        if final_join_results[row][pos_b2] == b2:
                            if final_join_results[row][pos_b1] == None:
                                # Add b2 to an empty place
                                new_row = final_join_results[row]
                                new_row[pos_b1] = b1
                                new_final_join_results.append(new_row)
                            elif final_join_results[row][pos_b1] != b1:
                                # Multiple b1 matches, add a new row
                                new_final_join_results.append(final_join_results[row])
                                new_row = final_join_results[row]
                                new_row[pos_b2] = b2
                                new_row[pos_b1] = b1
                                new_final_join_results.append(new_row)
                            else:
                                new_final_join_results.append(final_join_results[row])

            final_join_results = new_final_join_results
                        # TODO: this messes up the ordering - it should come after
                        # the row it was based on
                        #for new_row in new_final_join_results:
                            #final_join_results.append(new_final_join_results)

                
            # Clear up some space -- not using R.join_dict(table1, table2) anymore
            #R.join_dict[(table1, table2)] = []

        test_print('final_join_results length', len(final_join_results))

        # After all matching is done, remove any row in final_join_results containing None
        new_final_join_results = []
        for row in range(len(final_join_results)):
            none_row = False
            for i in final_join_results[row]:
                if i == None:
                    none_row = True
                    break
            if none_row == False:
                new_final_join_results.append(final_join_results[row])
        final_join_results = new_final_join_results
        
        # Get values for the byte positions from R.filtered_results_dict
        # Loop through all rows in final_join_results. Turn the byte numbers into data.
        for row in range(len(final_join_results)):
            # Loop through each table.attr in the SELECT clause.
            # Map byte number from final_join_results to table.attr in SELECT to data
            row_results = []
            for i in range(len(Q.SELECT)):
                table_attr = Q.SELECT[i]
                (table, attr_name) = parse_table_attribute_pair(table_attr)
                
                # Get position of table in final_join_results
                # join_table_list is like the header for final_join_results
                for t in range(len(join_table_list)):
                    if join_table_list[t] == table:
                        pos_b = t
                        break
                
                b = final_join_results[row][pos_b]
                for j in range(len(R.filtered_results_dict[table])):
                    if len(R.filtered_headers_dict[table]) > j:
                        if R.filtered_headers_dict[table][j] == table_attr:
                            # TODO: would be faster if there was a m
                            row_results.append(R.filtered_results_dict[table][b][j])
                            break

            if len(row_results) > 0:
                combined_results.append(row_results)
    
    R.final_results = combined_results

def project_row(q, table, mode='', row=[]):
    """PROJECT_ROW
        DESCRIPTION: Take a data row filtered by a query, return the elements of that row
            that are projected in the SELECT clause
        INPUT:
        OUTPUT: 
    """
    projected_results = []
    for i in range(len(q.SELECT)):
        # figure out which SELECT statements apply to this table
        table_attr = q.SELECT[i]
        table_attr_split = parse_table_attribute_pair(table_attr)
        if table_attr_split[0] == table:
            if mode == 'header':
                # Project the attribute names for this SELECT statement
                projected_results.append(table_attr)
            else:
                # figure out the index in the row that holds the desired attribute value
                attr_index = get_attribute_index(table_attr_split, q.attribute_dict)
                attr_value = row[attr_index]
                projected_results.append(attr_value)

    return projected_results


def count_join_tables(join_constraints):
    table_list = []
    for (table1, table2) in join_constraints:
        if table1 not in table_list:
            table_list.append(table1)
        if table2 not in table_list:
            table_list.append(table2)
    return len(table_list)


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
            attr_index = get_attribute_index(ta, q.attribute_dict)
            attr_value = row[attr_index]
            op = q.WHERE[i]['Verb']
            obj = q.WHERE[i]['Object']
            
            # Append result
            constraints_list.append(eval_binary_comparison(attr_value, op, obj))
                
        # only need to do extra processing if there are multiple WHERE conditions to combine, i.e., len >2
        #TODO: need to do some grouping when doing an OR compare on the same attribute
        result = constraints_list[0]
        if len(constraints_list) > 2:
            for c in range(len(constraints_list)):
                if c % 2 == 0 and c < len(constraints_list) - 2:
                    # Even numbers are results
                    a = result
                    op = constraints_list[c + 1]
                    b = constraints_list[c + 2]
                    result = eval_binary_comparison(a, op, b)

    return result

def get_individual_join_constraint_results(Q, table1, row1, table2, row2):
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
    for i in Q.join_constraints[table1, table2]:
        # Found a pair of tables in join_constraints. Because there may be multiple join
        # constraints for a table1-table2 pair, parse all join_constraints for this pair
        # into a single list built for comparison.

        # A connector is a boolean operator like AND that links multiple constraints.
        # The first one is always an empty string.
        constraint_results.append(Q.WHERE[i]['Connector'])

        table_attr1 = Q.WHERE[i]['Subject']
        attr_index1 = get_attribute_index(table_attr1, Q.attribute_dict)
        attr_value1 = row1[attr_index1]
        
        operator = Q.WHERE[i]['Verb']
        
        table_attr2 = Q.WHERE[i]['Object']
        attr_index2 = get_attribute_index(table_attr2, Q.attribute_dict)
        attr_value2 = row2[attr_index2]
        
        # Case for arithmetic in join
        no_compare = False
        if 'Operator' in Q.WHERE[i] and 'Operand' in Q.WHERE[i]:
            if attr_value1 != '' and attr_value2 != '':
                # TODO: sort this out better, like eval_binary_comparison
                math_operator = Q.WHERE[i]['Operator']
                operand = int(Q.WHERE[i]['Operand'])
                
                try:
                    
                    attr_value1 = int(attr_value1)
                except ValueError:
                    if attr_value1.strip('0123456789') == '.':
                        attr_value1 = float(attr_value1)
                    else:
                        no_compare = True
                
                try:
                    
                    attr_value2 = int(attr_value2)
                except ValueError:
                    if attr_value2.strip('0123456789') == '.':
                        attr_value2 = float(attr_value2)
                    else:
                        no_compare = True
            
                try:
                    
                    operand = int(operand)
                except ValueError:
                    if operand.strip('0123456789') == '.':
                        operand = float(operand)
                    else:
                        no_compare = True

                if no_compare == False:
                    if math_operator == '+':
                        attr_value2 += operand
                    if math_operator == '-':
                        attr_value2 -= operand
                    if math_operator == '*':
                        attr_value2 *= operand
                    if math_operator == '/':
                        attr_value2 /= operand
        
        # Append result
        constraint_results.append(eval_binary_comparison(attr_value1, operator, attr_value2))
        
    return constraint_results

def compare_join_constraints(Q, table1, row1, table2, row2):
    """COMPARE_JOIN_CONSTRAINTS
        DESCRIPTION: Handles the comparison of join constraints on a given table1-table2 pair.
            If there is only one join constraint, it returns the result. If there are more
            than one, it compares the results of each join constraint, based on the boolean
            operator that connects them
        INPUT:
            - string table: table name of data being tested
            - list row: data row from table
            - Query q: full user input query
        OUTPUT: boolean result
    """
    # Compare multiple join constraints on table1-table2, if there are any
    # only need to do extra processing if there are multiple WHERE conditions to combine.
    #TODO: need to do some grouping when doing an OR compare on the same attribute
    
    constraint_results = get_individual_join_constraint_results(Q, table1, row1, table2, row2)

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

            

    
class ResultsManager:
    def __init__(self):
        self.final_results = []
        self.filtered_headers_dict = {}
        self.filtered_results_dict = {}
        self.join_dict = {}
        self.zeropass_fitered_results_dict = {}
        self.zeropass_join_dict = {}