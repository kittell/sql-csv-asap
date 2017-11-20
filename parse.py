import utils

# CLASSES

# METHODS

def check_has_join(subject, object):
    """CHECK_HAS_JOIN
    DESCRIPTION: Determine whether SQL query has join in WHERE conditions. Used as a
        flag for separate query result processing.
    INPUT: 
        - subject of WHERE clause of SQL query
        - object of WHERE clause of SQL query
    OUTPUT: result: True if WHERE contains join; otherwise, False
    """
    
    # query has a join in it if:
    #   1) both the subject and object of any WHERE term has a '.' in it; AND
    #   2) substrings on both sides of '.' of any WHERE term are strings (i.e., not numbers)
    #   3) more than one table called in WHERE subject values (t1.attr, t2.attr)
    
    # TODO: This will find the . in a real number and say it's a join... fix that
    #   Testing 2017-11-19: .split the string. Check that both sides are not numbers.
    
    result = False
    has_dot = False
    split_not_number = False
    multi_table = False
    table_list = []
    
    # Test 1: Subject and Object have a dot
    # Test 2: not numbers on both sides of dot
    if '.' in subject:
        if '.' in object:
            has_dot = True
            split_not_number = True
            
            utils.test_print('check_has_join / has_dot', has_dot)
            
            subject_split = subject.split('.')
            for j in subject_split:
                if j.isnumeric() == True:
                    split_not_number = False
            
            object_split = object.split('.')
            for j in object_split:
                if j.isnumeric() == True:
                    split_not_number = False
            
            utils.test_print('check_has_join / split_not_number', split_not_number)
            
            # Test 3: more than one table in WHERE query
            if subject_split[0] != object_split[0]:
                multi_table = True
            utils.test_print('check_has_join / multi_table', multi_table)
    
    if has_dot == True and split_not_number == True and multi_table == True:
        result = True
    
    utils.test_print('check_has_join / result', result)
    
    return result

def parse_query(query_input):
    """PARSE_QUERY
    DESCRIPTION: Parse a SQL query input by user into its components (SELECT, FROM,
        WHERE, etc.) in a format that can be used as a query by the program.
    INPUT: query_input: string containing the entire SQL query
    OUTPUT: parsed_query: dict, with (key:value) as (SQL query components:values)
    """

    # Walk through query string
    # Everything between SQL terms becomes a candidate component
    #   e.g., substring between SELECT and FROM becomes SELECT query candidate
    # Candidates are decomposed into individual components
    
    # TODO: extend this to ORDER BY, GROUP BY, etc.
    query_terms_list = ['SELECT', 'FROM', 'WHERE']  # list of allowed query statements
    
    # Determines whether term has been found in query. Initialize to False.
    found_terms = {}
    for term in query_terms_list:
        found_terms[term] = False
        # See if terms are in the query input
        if (' ' + term + ' ') in query_input:
            # the space is there so that you don't find the name of a table or
            # attribute that happens to contain a query term
            found_terms[term] = True

    # ...but SELECT won't have a space in front of it, so set it anyway:
    found_terms['SELECT'] = True

    # Get starting string index for each term in string
    query_index = {}
    for term in query_terms_list:
        # Query must start with SELECT:
        if term == 'SELECT':
            query_index[term] = 0
        else:
            if found_terms[term] == True:
                for i in range(len(query_input)):
                    # protect against reading past end of string:
                    if len(query_input) - len(term) > 0:
                        if query_input[i:i+len(term)] == term:
                            # Found the term, grab index and break
                            query_index[term] = i
                            break

    # "query_candidate" means that, e.g., the entire SELECT statement will be taken
    # in one chunk. It will be broken down into individual components later
    query_candidates = {}
    for i in range(len(query_terms_list)):
        term = query_terms_list[i]
        if found_terms[term] == True:
            # Get the start and end position of each SQL statement value
            i_start = query_index[term] + len(term)
            if i == len(query_terms_list) - 1:
                # if it's the last term in the list: end of query_input string is
                # the end of the term value
                i_end = len(query_input)
            else:
                # otherwise, i_end is the character before the start of the next term
                if found_terms[query_terms_list[i + 1]] == True:
                    i_end = query_index[query_terms_list[i + 1]]
                else:
                    i_end = len(query_input)

            query_candidates[term] = query_input[i_start:i_end]


    # Print dict values to understand the intermediate calculations
    utils.test_print('parse_query / query_terms_list', query_terms_list)
    utils.test_print('parse_query / found_terms', found_terms)
    utils.test_print('parse_query / query_index', query_index)
    utils.test_print('parse_query / query_candidates', query_candidates)

    # parse_cmd helps to direct traffic for validating each SQL statement
    parse_cmd = {
        'SELECT': parse_select,
        'FROM': parse_from,
        'WHERE': parse_where
    }

    # TODO: next, parse each candidate term into the final dict that will be returned from this method
    parsed_query = {}
    for term in query_terms_list:
        if found_terms[term] == True:
            parsed_query[term] = parse_cmd[term](query_candidates[term])
        else:
            parsed_query[term] = ''
    
    # Further processing for SELECT statement
    # If the user selected *, explicitly add all attributes
    if parsed_query['SELECT'][0] == '*':
        # Assumption: getting all values from just one table
        csv_fullpath = utils.get_csv_fullpath(
            utils.table_to_csv(
                parsed_query['FROM'][0]
            )
        )
        parsed_query['SELECT'] = utils.get_attribute_list(csv_fullpath)
        utils.test_print('parse_query / parsed_query[SELECT]',parsed_query['SELECT'])

    # convert all attributes in SELECT to be table.attr pairs
    # TODO: this should be its own function -- out of time, doubling up for now
    for i in range(len(parsed_query['SELECT'])):
        # if it has a dot already, it's good to go
        select_attr = parsed_query['SELECT'][i]
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_query['FROM']:
                csv_fullpath = utils.get_csv_fullpath(
                    utils.table_to_csv(
                        table_name
                    )
                )
                attribute_list = utils.get_attribute_list(csv_fullpath)
                for attr in attribute_list:
                    if attr == select_attr:
                        ta = table_name + '.' + attr
                        found_attr_match = True
                        break

                if found_attr_match == True:
                    break
            
            parsed_query['SELECT'][i] = ta

    # convert all attributes in WHERE to be table.attr pairs
    # TODO: this should be its own function -- out of time, doubling up for now
    for i in range(len(parsed_query['WHERE'])):
        # if it has a dot already, it's good to go
        select_attr = parsed_query['WHERE'][i]['Subject']
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_query['FROM']:
                csv_fullpath = utils.get_csv_fullpath(
                    utils.table_to_csv(
                        table_name
                    )
                )
                attribute_list = utils.get_attribute_list(csv_fullpath)
                for attr in attribute_list:
                    if attr == select_attr:
                        ta = table_name + '.' + attr
                        found_attr_match = True
                        parsed_query['WHERE'][i]['Subject'] = ta
                        break

                if found_attr_match == True:
                    break

        
        utils.test_print('parse_query / parsed_query[WHERE][i][Subject]',parsed_query['WHERE'][i]['Subject'])   

    # convert all attributes in WHERE to be table.attr pairs
    # TODO: this should be its own function -- out of time, doubling up for now
    for i in range(len(parsed_query['WHERE'])):
        # if it has a dot already, it's good to go
        select_attr = parsed_query['WHERE'][i]['Object']
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in... if it is an attribute
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_query['FROM']:
                csv_fullpath = utils.get_csv_fullpath(
                    utils.table_to_csv(
                        table_name
                    )
                )
                attribute_list = utils.get_attribute_list(csv_fullpath)
                for attr in attribute_list:
                    if attr == select_attr:
                        ta = table_name + '.' + attr
                        found_attr_match = True
                        parsed_query['WHERE'][i]['Object'] = ta
                        break

                if found_attr_match == True:
                    break

        
        utils.test_print('parse_query / parsed_query[WHERE][i][Object]',parsed_query['WHERE'][i]['Object'])  
    
    utils.test_print('\nfinal parsed_query',parsed_query)
    
    return parsed_query


def parse_select(candidate):
    """FUNCTION_NAME
    DESCRIPTION: 
    INPUT: candidate SELECT clause of input SQL query
    OUTPUT: parsed_list: list of individual components of SELECT clause
    """
    
    # TODO: There should be validation to prove attributes exist
    candidate = candidate.strip()   # Remove leading, trailing spaces
    parsed_list = candidate.split(',')
    # Remove leading, trailing spaces from individual terms
    for i in range(len(parsed_list)):
        parsed_list[i] = parsed_list[i].strip()
    
    return parsed_list

def parse_from(candidate):
    """FUNCTION_NAME
    DESCRIPTION: 
    INPUT: candidate FROM clause of input SQL query
    OUTPUT: parsed_list: list of individual components of FROM clause
    """
    
    # TODO: There should be validation to prove tables exist
    candidate = candidate.strip()   # Remove leading, trailing spaces
    parsed_list = candidate.split(',')
    # Remove leading, trailing spaces from individual terms
    for i in range(len(parsed_list)):
        parsed_list[i] = parsed_list[i].strip()
        
        # Just in case user typed table_name .csv for FROM, remove .csv , don't want it
        if parsed_list[i].endswith('.csv'):
            parsed_list[i] = parsed_list[i].replace('.csv', '')
        
    return parsed_list

def parse_where(candidate):
    """PARSE_WHERE
    DESCRIPTION: 
    INPUT: candidate WHERE clause of input SQL query
    OUTPUT: parsed_list: list of individual components of WHERE clause
    """
    
    # WHERE is a list of dictionaries with components: connector, subject, verb, object
    # example: 
    #           x[WHERE]['Connector'] = 'AND'
    #           x[WHERE]['Subject'] = 'Name'
    #           x[WHERE]['Verb'] = '='
    #           x[WHERE]['Object'] = 'Bill'
    #           x[WHERE]['Join'] = True
    #TODO: each WHERE currently requires a parenthetical statement; fix later
    
    utils.test_print('parse_where / candidate', candidate)
    
    candidate = candidate.strip()   # Remove leading, trailing spaces
    # Walk through candidate string. Add each term between () as an item to list
    pre_parsed_list = []            # Splits the () components into list items
    parsed_connector_list = ['']    # no connector before the first term
    
    between_where_terms = False
    for i in range(len(candidate)):
        if candidate[i] == '(':
            i_start = i
            # add substring between where terms to connector list
            if between_where_terms == True:
                parsed_connector_list.append(candidate[i_end + 1:i_start])
            between_where_terms = False
        elif candidate[i] == ')':
            i_end = i
            pre_parsed_list.append(candidate[i_start + 1:i_end])
            between_where_terms = True
    
    utils.test_print('parse_where / pre_parsed_list', pre_parsed_list)
    
    # Remove leading and trailing spaces, parentheses
    remove_list = [' ', '(', ')']
    for i in range(len(pre_parsed_list)):
        for j in remove_list:
            pre_parsed_list[i] = pre_parsed_list[i].strip(j)
            parsed_connector_list[i] = parsed_connector_list[i].strip(j)
    
    # intermediate parsed list: int_parsed_list
    # Separate each pre_parsed_list WHERE entry into subject/verb/object
    # Start by finding the 'verb' - then add the things on the side to subject/object
    
    utils.test_print('parse_where / pre_parsed_list', pre_parsed_list)
    
    where_comparison_list = ['=', '<', '>', '<>', '<=', '>=']
    int_parsed_list = []
    for item in pre_parsed_list:
        inner_int_parsed_list = []
        for i in range(len(item)):
            # First, handle LIKE operator
            if 'LIKE' in item:
                if i < (len(item) - 4):
                    if item[i:i+4] == 'LIKE':
                        i_end = i + 3
                        
                        if i > 3 and item[i-4:i-1] == 'NOT':
                            i_start = i-4       # special case for NOT LIKE
                        else:
                            i_start = i
                        
                        verb = item[i_start:i_end + 1]
                        break
            elif item[i] in where_comparison_list:
                i_start = i
                # do a second search to see if this is part of a two-character comparison (e.g., <=)
                if item[i:i+2] in where_comparison_list:
                    i_end = i_start + 1
                else:
                    i_end = i_start
                verb = item[i_start:i_end + 1]
                break
        subject = item[0:i_start]
        object = item[i_end + 1:len(item)]
        
        join = check_has_join(subject, object)
        
        inner_int_parsed_list.append(subject)
        inner_int_parsed_list.append(verb)
        inner_int_parsed_list.append(object)
        inner_int_parsed_list.append(join)
        for i in range(len(inner_int_parsed_list)):
            if type(inner_int_parsed_list[i]) is str:
                # .strip() will fail on boolean 'Join'
                inner_int_parsed_list[i] = inner_int_parsed_list[i].strip()   # Remove leading, trailing spaces
        
        int_parsed_list.append(inner_int_parsed_list)
        
        utils.test_print('parse_where / inner_int_parsed_list', inner_int_parsed_list)
        
    utils.test_print('parse_where / int_parsed_list', int_parsed_list)
    
    final_parsed_list = []
    for i in range(len(int_parsed_list)):
        final_parsed_list.append(
            {
                'Connector': parsed_connector_list[i],
                'Subject': int_parsed_list[i][0],
                'Verb': int_parsed_list[i][1],
                'Object': int_parsed_list[i][2],
                'Join': int_parsed_list[i][3]
            }
        )
    utils.test_print('parse_where / final_parsed_list', final_parsed_list)
    
    return final_parsed_list
