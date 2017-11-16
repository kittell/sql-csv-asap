import os
import csv

# CLASSES

# METHODS

def get_user_query():
    print('\nEnter SELECT-FROM-WHERE query:')
    valid = False
    while valid == False:
        query_input = input('query > ')
        # parse query_input into SELECT, FROM, WHERE, etc.
        # validate components
        valid = True #temporary to get out of validation loop

    #user_query = query(inputSelect, inputFrom, inputWhere)
    user_query = query_input #temporary
    return user_query

def validate_user_command(user_command):
    # TODO - check if it's in the cmd dictionary
    # TODO - maybe also handle the capitalization
    valid = True
    return valid

def execute_user_command(user_command):
    cmd = {
        'help':cmd_help,
        'quit':cmd_quit,
        'query':cmd_query,
        'show tables':cmd_show_tables,
        'show attributes in':cmd_show_attributes
    }
    #TODO: something to handle an invalid command
    
    if 'show attributes in' in user_command:
        # get remainder of string after 'show attributes in'
        # TODO: for now, assume one space after 'show attributes in'; not nec. true
        table_name = user_command[len('show attributes in')+1:len(user_command)]
        user_command = 'show attributes in'
        return cmd[user_command](table_name)
    else:
        return cmd[user_command]()

def get_user_command():
    """
    Retrieve a command line command from user.
    Validate it, then pass it back.
    """
    valid = False
    while valid == False:
        user_command = input('> ')
        valid = validate_user_command(user_command)
    return user_command

WORKING_DIRECTORY = os.getcwd()
TABLE_DIRECTORY = WORKING_DIRECTORY + '\\tables'

def get_csv_list():
    # I don't think this is the best way to do this, but it works for now...
    # os.walk through /tables directory and add files to filenames list,
    # then walk back through filenames list and throw out everything that isn't a .csv
    # TODO: Probably it's best to not add a non-csv to the list in the first place...
    
    # https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory#3207973
    f = []
    for (dirpath, dirnames, filenames) in os.walk(TABLE_DIRECTORY):
        f.extend(filenames)
        break

    for entry in filenames:
        if '.csv' not in entry:
            filenames.remove(entry)

    return filenames

def csv_to_table(csv_filename):
    # csv_filename is the table name with the .csv extension
    # For now, assuming that all entries end in .csv, and removing last four char
    # probably should be a validation test just in case
    table_name = csv_filename[0:len(csv_filename)-4]
    return table_name

def table_to_csv(table_name):
    csv_filename = table_name + '.csv'
    return csv_filename

def parse_query(query_input):
    # Assumption: query has been validated prior to calling this function
    # Split query up into SELECT, FROM, and WHERE components
    # TODO: this only handles a single value for each component; need to fix it
    #   to handle multiple selects, froms, wheres, etc.

    # Assumption: SELECT and FROM are required
    # Assumption: SELECT FROM WHERE must be in order

    # Walk through query string
    # Everything between SELECT and FROM becomes query_select
    # Everything between FROM and WHERE, or FROM and end-of-string becomes query_from
    # And so on. Each term between the entries in query_terms_list below is associated
    #   with the term before it.

    # Also thought about having a Query object. Going with query_dict for now.
    query_dict = {}
    found = {}

    # list of terms to find in query
    query_terms_list = ['SELECT', 'FROM', 'WHERE']
    for term in query_terms_list:
        found[term] = False
        # See if terms are in the query input
        if (' ' + term + ' ') in query_input:
            # the space is there so that you don't find the name of a table or
            # attribute that contains select, from, etc.
            found[term] = True

        # ...but SELECT won't have a space in front of it:
        if query_input[0:len('select')] == 'SELECT':
            found['SELECT'] = True

    # Get startingindex for each term in string
    query_index = {}
    for term in query_terms_list:
        # Query must start with SELECT:
        if term == 'SELECT':
            query_index[term] = 0
        else:
            if found[term] == True:
                for i in range(len(query_input)):
                    # protect against reading past end of string:
                    if len(query_input) - len(term) > 0:
                        if query_input[i:i+len(term)] == term:
                            # Found the term, grab index and break
                            query_index[term] = i
                            break

    # TODO: get the candidate values for each term
    #   "candidates" means that the entire SELECT term will be taken,
    #   and then later have spaces removed and have multiple terms
    #   broken into individual list items
    query_term_candidates = {}
    for term in query_terms_list:
        if found[term] == True:
            i_start = query_index[term] + len(term) + 1 # +1 is to account for space
            # i_end: index of end of candidate term
            i_end = query_index[term] + 1
            query_term_candidates[term] = query_input[i_start:i_end + 1]

    for i in range(len(query_terms_list)):
        term = query_terms_list[i]
        if found[term] == True:
            i_start = query_index[term] + len(term)
            if i == len(query_terms_list) - 1:
                # if it's the last term in the list: end of query_input string is
                # the end of the term value
                i_end = len(query_input)
            else:
                # otherwise, it's the character before the start of the next term
                # ...unless the next term isn't in the query
                # TODO: need to fix this--broken logic, only works b/c there are a few terms
                if found[query_terms_list[i + 1]] == True:
                    i_end = query_index[query_terms_list[i + 1]]
                else:
                    i_end = len(query_input)

            query_term_candidates[term] = query_input[i_start:i_end]

    # TEMP: printing dict values to understand the intermediate calculations
    print('query_terms_list:', query_terms_list)
    print('found:', found)
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
        if found[term] == True:
            parsed_query[term] = parse_dict[term](query_term_candidates[term])

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

# QUERY METHODS

def get_comparison_function(c):
    #inspiration: https://stackoverflow.com/a/1740759/752784
    # operator library: https://docs.python.org/3/library/operator.html
    return {
            '=': operator.eq
        }[c]

def eval_binary_comparison(a, op, b):
    return get_comparison_function(op)(a, b)

def get_attribute_list(csv_fullpath):
    with open(csv_fullpath, newline='') as f:
        reader = csv.reader(f)
        attribute_list = next(reader)
    return attribute_list

def perform_query(query):
    # loop through tables (FROM)
    # test rows (WHERE)
    # print attribute values (SELECT)

    # Assumption: SELECT and FROM are valid
    csv_list = []
    for table in query['FROM']:
        csv_list.append(table_to_csv(table))

    for csv_file in csv_list:
        csv_fullpath = TABLE_DIRECTORY + '//' + csv_file
        attribute_list = get_attribute_list(csv_fullpath)
        with open(csv_fullpath, newline = '') as f:
            reader = csv.reader(f)
            for row in reader:
                # test each row against where criteria
                # 1) which attribute are we testing?
                # TODO: this is just comparing strings; fix that
                for i in range(len(query['WHERE'])):
                    for j in range(len(attribute_list)):
                        #print('attribute:',attribute)
                        #print('where/subject:',query['WHERE'][i]['Subject'])
                        if query['WHERE'][i]['Subject'] == attribute_list[j]:
                            # here's the test
                            op = query['WHERE'][i]['Verb']
                            val = query['WHERE'][i]['Object']
                            
                            #print(row[j])
                            #print(val)
                            # TODO: temp for developing
                            if row[j] == val:
                                for x in range(len(query['SELECT'])):
                                    for y in range(len(attribute_list)):
                                        if query['SELECT'][x] == '*':
                                            print(row[y])
                                        elif query['SELECT'][x] == attribute_list[y]:
                                            print(row[y])
                                            break
                                #print(attribute, op, val)
                            break

# COMMAND METHODS

def cmd_help():
    # Show list of commands, and how to use them
    print('The following commands are available:')
    print('query . . . . . . . . . . . . Go to the query prompt to run a SQL query')
    print('show tables . . . . . . . . . Show the available CSV files for querying')
    print('show attributes in [TABLE]  . Show the available attributes in a given table.')
    print('                              Do not include .csv at the end of the table name')
    print('quit  . . . . . . . . . . . . Quit the program')
    print('help  . . . . . . . . . . . . List functions available for use')
    print('\n')
    print('Query format:')
    print('SELECT [attr] FROM [table] WHERE [condition]')
    print('\n')
    return True

def cmd_quit():
    # Do nothing. Return False. This will cause the main loop to exit.
    print('***Goodbye***')
    return False

def cmd_query():
    # This is the signal to the program to collect a query from the user.
    user_query = get_user_query()
    parsed_query = parse_query(user_query)
    perform_query(parsed_query)

    # TODO: after getting the query, parse it into its components (select, from, where, etc.)
    return True

def cmd_show_tables():
    # show list of .csv files in /tables, one per line

    csv_list = get_csv_list()
    print('List of available tables:')
    for entry in csv_list:
        table_name = csv_to_table(entry)
        print('*', table_name)

    return True

def cmd_show_attributes(table_name):
    
    csv_filename = table_name + '.csv'
    csv_fullpath = TABLE_DIRECTORY + '\\' + csv_filename

    attribute_list = get_attribute_list(csv_fullpath)

    print('Attributes in', table_name)
    for attribute in attribute_list:
        print('*', attribute)

    return True

# VALIDATION METHODS

def validate_select(input):
    # Conditions for validity:
    # - Attribute must exist
    valid = False

    #TODO: hardcode True for now
    valid = True
    
    return valid

def validate_from(input):
    # Conditions for validity:
    # - Table must exist
    valid = False

    #TODO: hardcode True for now
    valid = True
    
    return valid

def validate_where(input):
    # Conditions for validity:
    # - Type must be correct
    valid = False

    #TODO: hardcode True for now
    valid = True
    return valid

def validate_query(valid_select, valid_from, valid_where):
    #TODO: should have some meaningful error messages
    valid = True
    if valid_select == False: valid = False
    if valid_from == False: valid = False
    if valid_where == False: valid = False

    return valid

# MAIN

if __name__ == '__main__':
    print('SQL CSV ASAP')

    # Keep running this loop while the user is using the program.
    # Only the command 'quit' should return False, thus exiting the loop.
    keep_going = True
    while keep_going == True:
        user_command = get_user_command()
        keep_going = execute_user_command(user_command)
