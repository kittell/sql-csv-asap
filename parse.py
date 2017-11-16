TESTPRINT = False

def parse_query(query_input):
    # Split query up into SELECT, FROM, and WHERE components
    # Assumption: SELECT and FROM are required - TODO: error if not

    # Walk through query string
    # Everything between SELECT and FROM becomes query_select
    # Everything between FROM and WHERE, or FROM and end-of-string becomes query_from
    # And so on. Each term between the entries in query_terms_list below is associated
    #   with the term before it.


    # TODO: extend this to ORDER BY, GROUP BY, etc.
    query_terms_list = ['SELECT', 'FROM', 'WHERE'] # List of query terms that can be processed
    found_terms = {}    # determines whether term has been found in query
    for term in query_terms_list:
        found_terms[term] = False
        # See if terms are in the query input
        if (' ' + term + ' ') in query_input:
            # the space is there so that you don't find the name of a table or
            # attribute that contains select, from, etc.
            found_terms[term] = True

        # ...but SELECT won't have a space in front of it:
        if query_input.startswith('SELECT'):
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

    #   "candidate" means that the entire SELECT value will be taken,
    #   and then later be parsed into individual values
    query_term_candidates = {}
    for term in query_terms_list:
        # Find the start and end of each candidate query component
        if found_terms[term] == True:
            i_start = query_index[term] + len(term) + 1 # +1 is to account for space
            i_end = query_index[term] + 1
            query_term_candidates[term] = query_input[i_start:i_end]
        else:
            # the 'none' value will be a signal later for the WHERE query
            query_term_candidates[term] = None

    for i in range(len(query_terms_list)):
        term = query_terms_list[i]
        if found_terms[term] == True:
            i_start = query_index[term] + len(term)
            if i == len(query_terms_list) - 1:
                # if it's the last term in the list: end of query_input string is
                # the end of the term value
                i_end = len(query_input)
            else:
                # otherwise, it's the character before the start of the next term
                # ...unless the next term isn't in the query
                # TODO: need to fix this--broken logic, only works b/c there are a few terms
                if found_terms[query_terms_list[i + 1]] == True:
                    i_end = query_index[query_terms_list[i + 1]]
                else:
                    i_end = len(query_input)

            query_term_candidates[term] = query_input[i_start:i_end]
        else:
            found_terms[term] = None

    # TEMP: printing dict values to understand the intermediate calculations
    if TESTPRINT == True:
        print('query_terms_list:', query_terms_list)
        print('found_terms:', found_terms)
        print('query_index:', query_index)
        print('query_term_candidates:', query_term_candidates)

    parse_dict = {
        'SELECT': parse_select,
        'FROM': parse_from,
        'WHERE': parse_where
    }

    # TODO: next, parse each candidate term into the final dict that will be returned from this method
    parsed_query = {}
    for term in query_terms_list:
        if found_terms[term] == True:
            parsed_query[term] = parse_dict[term](query_term_candidates[term])
        else:
            parsed_query[term] = ''
    if TESTPRINT == True:
        print(parsed_query)
    
    return parsed_query

def parse_select(candidate):
    candidate = candidate.strip()
    parsed_list = candidate.split(',')
    # Remove leading, trailing spaces
    for i in range(len(parsed_list)):
        parsed_list[i] = parsed_list[i].strip()
    return parsed_list

def parse_from(candidate):
    candidate = candidate.strip()
    parsed_list = candidate.split(',')
    # Remove leading, trailing spaces
    for i in range(len(parsed_list)):
        parsed_list[i] = parsed_list[i].strip()
    return parsed_list

where_operator_list = [ '=', '<>', '<', '<=', '>', '>=' ]

def parse_where(candidate):
    # WHERE is a list of dictionaries with components: subject, verb, object
    candidate = candidate.strip()
    parsed_predict_list = candidate.split(' ')
    # Remove leading, trailing spaces
    for i in range(len(parsed_predict_list)):
        parsed_predict_list[i] = parsed_predict_list[i].strip()

    # TODO: making a lot of assumptions for demo code; fix later
    # Assumption: getting a single where statement with three components
    parsed_list = [
        {
            'Subject': parsed_predict_list[0],
            'Verb': parsed_predict_list[1],
            'Object': parsed_predict_list[2]
        }
    ]
    
    return parsed_list