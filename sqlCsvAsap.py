import os
import csv
import parse
import query
import utils
import time

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
        user_command = input('\n> ')
        valid = validate_user_command(user_command)
    return user_command


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
    print()
    print('Query format:')
    print('SELECT [attr] FROM [table] WHERE ([condition])')
    print()
    print('Note: Each WHERE condition must be contained in parentheses.')
    print('Multiple WHERE conditions can be separated by logical operators:')
    print('    WHERE (Year = 2000) AND (Value < 200)')
    print()
    return True

def cmd_quit():
    # Do nothing. Return False. This will cause the main loop to exit.
    print('\n***Goodbye***\n')
    return False

def cmd_query():
    # This is the signal to the program to collect a query from the user.
    user_query = get_user_query()
    
    # START TIMER - after receiving user query
    start_time = time.time()
    
    parsed_query = parse.parse_query(user_query)
    query_result_list = query.perform_query(parsed_query)
    utils.display_query_result(query_result_list)
    
    # END TIMER - after displaying query
    print("--- %s seconds ---" % (time.time() - start_time))
    
    return True

def cmd_show_tables():
    # show list of .csv files in /tables, one per line

    csv_list = utils.get_csv_list()
    print('List of available tables:')
    for entry in csv_list:
        table_name = utils.csv_to_table(entry)
        print('*', table_name)

    return True

def cmd_show_attributes(table_name):
    
    csv_filename = table_name + '.csv'
    csv_fullpath = os.path.join(utils.get_table_directory(), csv_filename)

    attribute_list = utils.get_attribute_list(csv_fullpath)

    print('Attributes in', table_name)
    for attribute in attribute_list:
        print('*', attribute)

    return True


# MAIN

if __name__ == '__main__':
    print('!!!SQL CSV ASAP!!!')
    print('\nType help for a list of commands')

    # Keep running this loop while the user is using the program.
    # Only the command 'quit' should return False, thus exiting the loop.
    keep_going = True
    while keep_going == True:
        user_command = get_user_command()
        try:
            keep_going = execute_user_command(user_command)
        except KeyError:
            print('Invalid command. Try again. Type "help" for a list of commands.')
