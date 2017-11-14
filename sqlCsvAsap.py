import os

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

def get_csv_list():
    current_directory = os.getcwd()
    table_directory = current_directory + '\\tables'

    # I don't think this is the best way to do this, but it works for now...
    # os.walk through /tables directory and add files to filenames list,
    # then walk back through filenames list and throw out everything that isn't a .csv
    # TODO: Probably it's best to not add a non-csv to the list in the first place...
    
    # https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory#3207973
    f = []
    for (dirpath, dirnames, filenames) in os.walk(table_directory):
        f.extend(filenames)
        break

    for entry in filenames:
        if '.csv' not in entry:
            filenames.remove(entry)

    return filenames

# COMMAND METHODS

def cmd_help():
    # Show list of commands, and how to use them
    print('TODO: description of available functions and how to use them')
    return True

def cmd_quit():
    # Do nothing. Return False. This will cause the main loop to exit.
    print('Goodbye.')
    return False

def cmd_query():
    # This is the signal to the program to collect a query from the user.
    user_query = get_user_query()

    # TODO: after getting the query, parse it into its components (select, from, where, etc.)
    return True

def cmd_show_tables():
    # show list of .csv files in /tables, one per line

    csv_list = get_csv_list()
    print('List of available tables:')
    for entry in filenames:
        # table_name is the filename minus .csv
        # For now, assuming that all entries end in .csv, although there should be a test for it
        table_name = entry[0:len(entry)-4]
        print('*', table_name)

    return True

def cmd_show_attributes(table_name):
    #TODO: pull values from first line of table_name.csv
    print('TODO: show attributes in', table_name)
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
