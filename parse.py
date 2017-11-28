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

def parse_query_into_candidates(query_input):
    """PARSE_CANDIDATES
    DESCRIPTION: 
    INPUT: query_input: 
    OUTPUT: query_candidates: 
    """
    # list of allowed query statements
    query_terms_list = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY']  
    
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
        
        # Add an empty string for all terms--missing terms may cause problems in query
        query_candidates[term] = ''
        
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
    utils.test_print('parse_candidates / query_terms_list', query_terms_list)
    utils.test_print('parse_candidates / found_terms', found_terms)
    utils.test_print('parse_candidates / query_index', query_index)
    utils.test_print('parse_candidates / query_candidates', query_candidates)
    
    return query_candidates
    
def parse_query(query_input):
    """PARSE_QUERY
    DESCRIPTION: Parse a SQL query input by user into its components (SELECT, FROM,
        WHERE, etc.) in a format that can be used as a query by the program.
    INPUT: query_input: string containing the entire SQL query
    OUTPUT: parsed_query: dict, with (key:value) as (SQL query components:values)
    """

    # Walk through query string
    # Everything between SQL terms becomes a candidate for the preceding clause
    #   e.g., substring between SELECT and FROM becomes SELECT query candidate
    # Candidates are decomposed into individual components
    # So, an example query might be parsed like this:
    #   SELECT Name, Bib FROM BostonMarathon2017 WHERE (Country = KEN) AND (Gender = F)
    #       ...is parsed into the following dict...
    #   {
    #       'SELECT': ['Name', 'Bib'],
    #       'FROM': ['BostonMarathon2017'],
    #       'WHERE': [
    #           {
    #               'Connector': ''
    #               'Subject': 'Country'
    #               'Verb': '='
    #               'Object': 'KEN'
    #               'Join': False
    #           },
    #           {
    #               'Connector': 'AND'
    #               'Subject': 'Gender'
    #               'Verb': '='
    #               'Object': 'F'
    #               'Join': False
    #           },
    #       ]   
    #   }
    
    # Break input query string into different clauses
    # 'candidate' indicates that the entire substring following a SQL statement will
    # be included, e.g., from the input SELECT Name, Bib FROM ..., the candidate SELECT
    # clause will be 'Name, Bib'
    # Further processing of candidates will parse out multiple statements
    # from an individual candidate.
    query_candidates = {}
    query_candidates = parse_query_into_candidates(query_input)

    # parse_cmd helps to direct traffic for validating each SQL statement
    parse_cmd = {
        'SELECT': parse_select,
        'FROM': parse_from,
        'WHERE': parse_where
    }

    parsed_query = {}
    for term in query_candidates:
        parsed_query[term] = ''
        if query_candidates[term] != '':
            parsed_query[term] = parse_cmd[term](query_candidates)

    utils.test_print('parse_query / parsed_query', parsed_query)
    
    return parsed_query


def parse_select(query_candidates):
    """PARSE_SELECT
    DESCRIPTION: 
    INPUT: candidate SELECT clause of input SQL query
    OUTPUT: parsed_select_list: list of individual components of SELECT clause
    """
    # TODO: There should be validation to prove attributes exist
    candidate = query_candidates['SELECT'].strip()   # Remove leading, trailing spaces
    parsed_select_list = candidate.split(',')
    
    # Need a parsed FROM query to do this one.
    # TODO: so that means we're parsing FROM twice. Combine it somehow.
    parsed_from_list = parse_from(query_candidates)
    
    # Remove leading, trailing spaces from individual terms
    for i in range(len(parsed_select_list)):
        parsed_select_list[i] = parsed_select_list[i].strip()
    
    # If the user selected *, explicitly add all attributes
    if parsed_select_list[0] == '*':
        # Assumption: when SELECT is *, only receiving a single table in FROM statement,
        #   i.e., don't need to parse FROM into individual components, there is only one
        csv_fullpath = utils.get_csv_fullpath(
            utils.table_to_csv(parsed_from_list[0])
        )
        parsed_select_list = utils.get_attribute_list(csv_fullpath)
        utils.test_print('parse_select / parsed_select_list',parsed_select_list)
    
    # convert all attributes in SELECT to be table.attr pairs
    # TODO: this should be its own function, b/c it's also used in WHERE statement
    for i in range(len(parsed_select_list)):
        # if it has a dot already, it's good to go
        select_attr = parsed_select_list[i]
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_from_list:
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
            
            parsed_select_list[i] = ta
            utils.test_print('parse_select / parsed_select_list[i]',parsed_select_list[i])
    
    return parsed_select_list

def parse_from(query_candidates):
    """PARSE_FROM
    DESCRIPTION: 
    INPUT: candidate FROM clause of input SQL query
    OUTPUT: parsed_from_list: list of individual components of FROM clause
    """
    
    # TODO: There should be validation to prove tables exist
    candidate = query_candidates['FROM'].strip()   # Remove leading, trailing spaces
    parsed_from_list = candidate.split(',')
    # Remove leading, trailing spaces from individual terms
    for i in range(len(parsed_from_list)):
        parsed_from_list[i] = parsed_from_list[i].strip()
        
        # Just in case user typed table_name .csv for FROM, remove .csv , don't want it
        if parsed_from_list[i].endswith('.csv'):
            parsed_from_list[i] = parsed_from_list[i].replace('.csv', '')
        
    return parsed_from_list

def parse_where(query_candidates):
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
    
    utils.test_print('parse_where / query_candidates[WHERE]', query_candidates['WHERE'])
    
    candidate = query_candidates['WHERE'].strip()   # Remove leading, trailing spaces
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
    
    # Need a parsed FROM query to do this one.
    # TODO: so that means we're parsing FROM twice. Combine it somehow.
    parsed_from_list = parse_from(query_candidates)
    
    # convert all attributes in WHERE to be table.attr pairs
    # TODO: this should be its own function -- out of time, doubling up for now
    for i in range(len(final_parsed_list)):
        print('i:', i)
        # if it has a dot already, it's good to go
        select_attr = final_parsed_list[i]['Subject']
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_from_list:
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
                        final_parsed_list[i]['Subject'] = ta
                        break

                if found_attr_match == True:
                    break

        
        utils.test_print('parse_query / final_parsed_list[i][Subject]',final_parsed_list[i]['Subject'])   

    # convert all attributes in WHERE to be table.attr pairs
    # TODO: this should be its own function -- out of time, doubling up for now
    for i in range(len(final_parsed_list)):
        # if it has a dot already, it's good to go
        select_attr = final_parsed_list[i]['Object']
        if '.' not in select_attr:
            # ta is table.attr pair
            ta = ''
            attribute_list = []
            # need to figure out what table each attribute is in... if it is an attribute
            # Assumption: use the first one you find; user should do a better input if it doesn't work
            
            found_attr_match = False
            for table_name in parsed_from_list:
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
                        final_parsed_list[i]['Object'] = ta
                        break

                if found_attr_match == True:
                    break

        
        utils.test_print('parse_query / final_parsed_list[i][Object]',final_parsed_list[i]['Object'])  
    
    
    return final_parsed_list
