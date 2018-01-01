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
    
    # Query timer for debugging
    checkpoint_time = time.time()

    R = ResultsManager()
    I.load_blank_indexes(Q)
    (start_time, checkpoint_time) = pause_printtime('load_blank_indexes:', checkpoint_time)
    I.load_value_constraint_indexes(Q)
    (start_time, checkpoint_time) = pause_printtime('load_value_constraint_indexes:', checkpoint_time)
    
    I.build_bytelists(Q)
    for t in I.bytelist_dict:
        test_print('\nNumber of ByteList entries',(t, len(I.bytelist_dict[t].bytes)))
    (start_time, checkpoint_time) = pause_printtime('build_bytelists:', checkpoint_time)
    
    # For each table, get the results filtered by value_constraints in the WHERE clause
    filter_value_constraints(Q, I, R)
    (start_time, checkpoint_time) = pause_printtime('filter_value_constraints:', checkpoint_time)
    
    # For each table pair, get the table1-table2 pairs resulting from join_constraints in WHERE clause
    filter_join_constraints(Q, I, R)
    (start_time, checkpoint_time) = pause_printtime('filter_join_constraints:', checkpoint_time)
    
    test_print('\nR.filtered_headers_dict',R.filtered_headers_dict)
    test_print('\nR.filtered_results_dict',R.filtered_results_dict)
    test_print('\nR.join_dict',R.join_dict)
    
    # Combine the results
    combine_final_results(Q, R)
    (start_time, checkpoint_time) = pause_printtime('combine_final_results:', checkpoint_time)
    
    # Handle ORDER BY
    order_final_results(Q, R)
    (start_time, checkpoint_time) = pause_printtime('order_final_results:', checkpoint_time)
    
    # Handle SELECT DISTINCT
    distinct_final_results(Q, R)
    (start_time, checkpoint_time) = pause_printtime('distinct_final_results:', checkpoint_time)
    
    return R.final_results

def filter_join_constraints(Q, I, R):
    """
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    # TODO: Replaced table references with table alias references to account for problem
    #   with not being able to self-join tables. However, have created a problem: code
    #   will treat multiple table aliases for the same table as separate tables, which 
    #   will require multiple I/O for the same table. Modify the loop later to grab
    #   both aliases' worth of info from a single table on a single run
    
    join_results_dict = {}
    #join_results_dict[table_alias1, table_alias2] = [(b1, b2)]
    
    # Build list of table aliases to read. Aliases will be converted to table names as needed
    table_alias1_list = []
    for table_alias_pair in Q.join_constraints:
        if table_alias_pair not in R.join_dict:
            R.join_dict[table_alias_pair] = []
        (table_alias1, table_alias2) = table_alias_pair
        if table_alias1 not in table_alias1_list:
            table_alias1_list.append(table_alias1)
    
    new_filtered_dict = {}
    new_filtered_dict_headers = {}
    
    for table_alias1 in table_alias1_list:
        has_bytelist1 = I.check_bytelist(table_alias1)
        
        if table_alias1 not in new_filtered_dict:
            new_filtered_dict[table_alias1] = {}
            new_filtered_dict_headers[table_alias1] = project_row(Q, table_alias1, 'header')
        
        # Loop over table1 -- converting table alias to table name via Q.alias dict
        csv_fullpath1 = table_to_csv_fullpath(Q.alias[table_alias1])
        f1 = open(csv_fullpath1, 'rb')
        b1 = 0
        i1 = 0

        while True:
            # If this table has a bytelist defined, use it to go to the next position;
            # otherwise, will scan table line-by-line.
            if has_bytelist1 == True:
                if len(I.bytelist_dict[table_alias1].bytes) > 0:
                    b1 = I.bytelist_dict[table_alias1].bytes[i1]
                    i1 += 1
        
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
                    for table_alias_pair in Q.join_constraints:
                        # Only check constraints where table1 is the first table
                        if table_alias_pair[0] != table_alias1:
                            continue
                        
                        table_alias2 = table_alias_pair[1]
                        has_bytelist2 = I.check_bytelist(table_alias2)

                        if table_alias2 not in new_filtered_dict:
                            new_filtered_dict[table_alias2] = {}
                            new_filtered_dict_headers[table_alias2] = project_row(Q, table_alias2, 'header')
                        
                        # Open table2 for reading
                        csv_fullpath2 = table_to_csv_fullpath(Q.alias[table_alias2])
                        f2 = open(csv_fullpath2, 'rb')
                        
                        for c2 in Q.join_constraints[table_alias_pair]:
                            [table_alias1, attr_name1] = parse_table_attribute_pair(Q.WHERE[c2]['Subject'])
                            #attr_index1 = Q.get_attribute_dict_index(combine_table_attribute_pair(table_alias1, attr_name1))
                            #attr_value1 = row1[attr_index1]
                            [table_alias2, attr_name2] = parse_table_attribute_pair(Q.WHERE[c2]['Object'])

                            # Case c: If bytelist2 is still None at this point, need to do a table scan on table2
                            
                            # Loop over table2
                            b2 = 0
                            i2 = 0
                            int_result = []
                            while True:
                                # If this table has a bytelist defined, use it to go to the next position;
                                # otherwise, will scan table line-by-line.
                                if has_bytelist2 == True:
                                    if len(I.bytelist_dict[table_alias2].bytes) > 0:
                                        b2 = I.bytelist_dict[table_alias2].bytes[i2]
                                        i2 += 1

                                # Return the line from position b2 in the file
                                f2.seek(b2)
                                (b2_returned, line2) = readline_like_csv(f2)
                                
                                # Line will read empty at end of file for table scan
                                if not line2:
                                    break
                    
                                # Skip first row (header) and blank rows
                                if b2 > 0 and line2 != '':
                                    for row2 in csv.reader([line2]):
                                        if compare_join_constraints(Q, table_alias1, row1, table_alias2, row2) == True:
                                            # row1-row2 passes join constraints. Capture the results.
                                            R.join_dict[table_alias_pair].append((b1, b2))
                                            # Add to filter_dict[table2] if there was no bytelist before starting.
                                            if b2 not in new_filtered_dict[table_alias2]:
                                                new_filtered_dict[table_alias2][b2] = project_row(Q, table_alias2, '', row2)
                                            if b1 not in new_filtered_dict[table_alias1]:
                                                new_filtered_dict[table_alias1][b1] = project_row(Q, table_alias1, '', row1)

                                # Check if bytelist2 has been exhausted -- if so, exit
                                if I.bytelist_dict[table_alias2].bytes != None:
                                    if i2 == len(I.bytelist_dict[table_alias2].bytes):
                                        break
                                b2 = b2_returned
                        f2.close()

            # Check if bytelist1 has been exhausted -- if so, exit
            if I.bytelist_dict[table_alias1].bytes != None:
                if i1 == len(I.bytelist_dict[table_alias1].bytes):
                    break
            b1 = b1_returned
    
        f1.close()
        test_print('\nNumber of results filtered by filter_join_constraints',(table_alias1, len(R.filtered_results_dict[table_alias1])))
        test_print('\nNumber of results filtered by filter_join_constraints',(table_alias2, len(R.filtered_results_dict[table_alias2])))
        
    # Feed new_filtered_dict into R.filtered_results_dict
    for table_alias in new_filtered_dict:
        if table_alias not in R.filtered_results_dict:
            R.filtered_results_dict[table_alias] = {}
        if table_alias not in R.filtered_headers_dict:
            R.filtered_headers_dict[table_alias] = {}
        R.filtered_results_dict[table_alias] = new_filtered_dict[table_alias]
        R.filtered_headers_dict[table_alias] = new_filtered_dict_headers[table_alias]
    
    
def filter_value_constraints(Q, I, R):
    """
    DESCRIPTION: 
    INPUT: 
    OUTPUT: 
    """
    # TODO: Need to handle case for no WHERE constraints...
    #filtered_results_dict[table_alias1][b] = [data]
    
    # Build list of table aliases to read. Aliases will be converted to table names as needed
    table_alias_list = []
    for table_alias in Q.value_constraints:
        if table_alias not in table_alias_list:
            table_alias_list.append(table_alias)
    
    for table_alias in table_alias_list:
        if table_alias in Q.value_constraints or len(Q.value_constraints) == 0:
            test_print('filter_value_constraints',table_alias)
            # Set up dict to hold results
            if table_alias not in R.filtered_results_dict:
                R.filtered_results_dict[table_alias] = {}
                
                # flag: If you get to the end of this filter, and there are no results
                flag_no_results = True
            
            # Calculate headers
            R.filtered_headers_dict[table_alias] = project_row(Q, table_alias, 'header')
            
            has_bytelist = I.check_bytelist(table_alias)
            
            # Open the table_alias file for reading
            csv_fullpath = table_to_csv_fullpath(Q.alias[table_alias])
            f = open(csv_fullpath, 'rb')
            b = 0
            i = 0
            while True:
                # If this table has a bytelist defined, use it to go to the next position;
                # otherwise, will scan table line-by-line.
                #test_print('bytelist_dict.bytes',(table_alias, I.bytelist_dict[table_alias].bytes))
                if has_bytelist == True:
                    if len(I.bytelist_dict[table_alias].bytes) > 0:
                        b = I.bytelist_dict[table_alias].bytes[i]
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
                        if test_value_constraints(table_alias, row, Q) == True:
                            int_select_results = project_row(Q, table_alias, '', row)
                        if len(int_select_results) > 0:
                            R.filtered_results_dict[table_alias][b] = int_select_results
                            flag_no_results = False
                
                # Index will be out of range at end of index scan
                if has_bytelist == True:
                    if i == len(I.bytelist_dict[table_alias].bytes):
                        break
                
                b = b_returned
            f.close()
            
            # If zero results are returned, that means none met the criteria
            # To signal this, set the return value to None
            if flag_no_results == True:
                R.filtered_results_dict[table_alias] = None
                
            # If I.bytelist_dict[table_alias].bytes is None, or if it is longer than the number
            # of filtered results returned, replace the bytelist with the filtered list. It
            # will make the next calculation faster in join constraints.
#            if I.bytelist_dict[table_alias].bytes == None or len(I.bytelist_dict[table_alias].bytes) > len(R.filtered_results_dict[table_alias]):
            if I.bytelist_dict[table_alias].bytes == None:
                filtered_list = []
                for b in R.filtered_results_dict[table_alias]:
                    if b not in filtered_list:
                        filtered_list.append(b)
                I.bytelist_dict[table_alias].bytes = filtered_list
                
        test_print('\nNumber of results filtered by filter_value_constraints',(table_alias, len(R.filtered_results_dict[table_alias])))
    

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
        table_alias_attr = Q.SELECT[i]
        header.append(table_alias_attr)
    combined_results.append(header)
    
    # CASE 1: NO JOIN.
    # Get the data values associated with the byte numbers
    if len(R.join_dict) == 0:
        for table_alias in R.filtered_results_dict:
            for b in R.filtered_results_dict[table_alias]:
                # For each byte position in this table_alias: 
                row_results = []
                for i in range(len(Q.SELECT)):
                    # For each table_alias_attr in SELECT, get the data from the current
                    # row in filtered_results_dict for this SELECT attribute. This is a final
                    # projection of the data.
                    # TODO: This might not even be necessary for NO JOIN case if
                    #   project was done earlier....
                    table_alias_attr_select = Q.SELECT[i]
                    # From R.filtered_headers_dict, find position in R.filtered_results_dict
                    # that corresponds to this SELECT table_alias_attr
                    (table_alias_select, attr_name_select) = parse_table_attribute_pair(table_alias_attr_select)
                    for j in range(len(R.filtered_headers_dict[table_alias])):
                        if R.filtered_headers_dict[table_alias][j] == attr_name_select:
                            # Found the attr_name_select location in headers.
                            # Now get the data from R.filtered_results_dict.
                            row_results.append(R.filtered_results_dict[table_alias][b][j])
                            break
                if len(row_results) > 0:
                    combined_results.append(row_results)
    
    # CASE 2: JOIN. Combine join constraints with filtered data.
    else:
        # General case: For each (table_alias1,table_alias2) pair in R.join_dict:
        #   For each (b1,b2) pair in R.join_dict[(table_alias1,table_alias2)]
        #       Search out matching results in other R.join_dict[(table_alias,table_alias)] pairs
        # e.g., for a three table join, need to match b2 of R.join_dict[(table_alias1,table_alias2)] = (b1,b2)
        # with b2 of R.join_dict[(table_alias2,table_alias3)] = (b2,b3)
        # End result will be list of [b1,b2,b3] from which to gather data from R.filtered_results_dict
                
        final_join_results = []
        # join_table_alias_list is a map to table headers
        join_table_alias_list = []
        for (table_alias1, table_alias2) in Q.join_constraints:
            if table_alias1 not in join_table_alias_list:
                join_table_alias_list.append(table_alias1)
            if table_alias2 not in join_table_alias_list:
                join_table_alias_list.append(table_alias2)
                
        n_join_tables = len(join_table_alias_list)
        joined_tables = []
        for (table_alias1, table_alias2) in R.join_dict:
            # Get position of table_alias1, table_alias2 in join_table_alias_list
            for t in range(len(join_table_alias_list)):
                if table_alias1 == join_table_alias_list[t]:
                    pos_b1 = t
                elif table_alias2 == join_table_alias_list[t]:
                    pos_b2 = t
        
            # Case: final_join_results is empty, add all table_alias1:b1, table_alias2:b2 for this (table_alias1, table_alias2)
            # Also: it's the full solution for a two-table join
            join_results_empty = False
            if len(final_join_results) == 0:
                join_results_empty = True

            new_final_join_results = []
            for (b1, b2) in R.join_dict[(table_alias1, table_alias2)]:
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

                
            # Clear up some space -- not using R.join_dict(table_alias1, table_alias2) anymore
            #R.join_dict[(table_alias1, table_alias2)] = []

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
            # Loop through each table_alias.attr in the SELECT clause.
            # Map byte number from final_join_results to table_alias.attr in SELECT to data
            row_results = []
            for i in range(len(Q.SELECT)):
                table_attr = Q.SELECT[i]
                (table_alias, attr_name) = parse_table_attribute_pair(table_attr)
                
                # Get position of table_alias in final_join_results
                # join_table_alias_list is like the header for final_join_results
                for t in range(len(join_table_alias_list)):
                    if join_table_alias_list[t] == table_alias:
                        pos_b = t
                        break
                
                b = final_join_results[row][pos_b]
                for j in range(len(R.filtered_results_dict[table_alias][b])):
                    #if len(R.filtered_headers_dict[table_alias]) > j:
                    if R.filtered_headers_dict[table_alias][j] == attr_name:
                        row_results.append(R.filtered_results_dict[table_alias][b][j])
                        break

            if len(row_results) > 0:
                combined_results.append(row_results)
    
    R.final_results = combined_results

def project_row(Q, table_alias, mode='', row=[]):
    """PROJECT_ROW
        DESCRIPTION: Take a data row filtered by a query, return the elements of that row
            that are projected in the SELECT clause
        INPUT:
        OUTPUT: 
    """
    projected_results = []
    for i in range(len(Q.SELECT)):
        # figure out which SELECT statements apply to this table
        table_alias_attr = Q.SELECT[i]
        table_alias_attr_split = parse_table_attribute_pair(table_alias_attr)
        if table_alias_attr_split[0] == table_alias:
            if mode == 'header':
                # Project the attribute names for this SELECT statement
                projected_results.append(table_alias_attr_split[1])
            else:
                # figure out the index in the row that holds the desired attribute value
                attr_index = Q.get_attribute_dict_index(table_alias_attr)
                attr_value = row[attr_index]
                projected_results.append(attr_value)

    return projected_results


def test_value_constraints(table_alias, row, Q):
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
    
    # loop through value_constraints that apply to this table_alias
    # If there are none, that's OK: pass it.
    
    if table_alias in Q.value_constraints:
        constraint = 0
        constraints_list = []
        for i in Q.value_constraints[table_alias]:
            # even number constraints are results of individual value_constraint tests
            # odd numbers are boolean connectors between them
            
            constraint = constraint + 1
            if constraint > 1:
                # A connector is a boolean operator like AND. Only relevant if there is more than
                # one value constraint for a single table_alias. Append to constraints_list in 
                # even-numbered slot.
                constraints_list.append(Q.WHERE[i]['Connector'])
        
            table_alias_attr = parse_table_attribute_pair(Q.WHERE[i]['Subject'])
            attr_index = Q.get_attribute_dict_index(table_alias_attr)
            attr_value = row[attr_index]
            op = Q.WHERE[i]['Verb']
            obj = Q.WHERE[i]['Object']
            
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

def get_individual_join_constraint_results(Q, table_alias1, row1, table_alias2, row2):
    """GET_INDIVIDUAL_JOIN_CONSTRAINT_RESULTS
        DESCRIPTION: For a given table1 and table2, find join constraints from the WHERE
            clause of the user query that involve table_alias1 on left side and table_alias2 on right side.
        INPUT:
            - string table: table name of data being tested
            - list row: data row from table
            - Query q: full user input query
        OUTPUT: list constraints_list: 
    """
    constraint_results = []
    for i in Q.join_constraints[table_alias1, table_alias2]:
        # Found a pair of tables in join_constraints. Because there may be multiple join
        # constraints for a table_alias1-table_alias2 pair, parse all join_constraints for this pair
        # into a single list built for comparison.

        # A connector is a boolean operator like AND that links multiple constraints.
        # The first one is always an empty string.
        constraint_results.append(Q.WHERE[i]['Connector'])

        table_alias_attr1 = Q.WHERE[i]['Subject']
        attr_index1 = Q.get_attribute_dict_index(table_alias_attr1)
        attr_value1 = row1[attr_index1]
        
        operator = Q.WHERE[i]['Verb']
        
        table_alias_attr2 = Q.WHERE[i]['Object']
        attr_index2 = Q.get_attribute_dict_index(table_alias_attr2)
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

def compare_join_constraints(Q, table_alias1, row1, table_alias2, row2):
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
    # Compare multiple join constraints on table_alias1-table_alias2, if there are any
    # only need to do extra processing if there are multiple WHERE conditions to combine.
    #TODO: need to do some grouping when doing an OR compare on the same attribute
    
    constraint_results = get_individual_join_constraint_results(Q, table_alias1, row1, table_alias2, row2)

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