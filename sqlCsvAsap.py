import os
import csv
from parse import *
from query import *
from utils import *
from index import *
import time

def get_user_query():
    print('\nEnter SELECT-FROM-WHERE query:\n')
    
    valid_query = False
    while valid_query == False:
        user_query = input('query > ')

        # Query validation
        if user_query == '':
            # Blank query exits query command line
            valid_query = True
        elif 'SELECT ' not in user_query:
            valid_query = False
            print('\nQuery must include SELECT statement\n')
        elif 'FROM ' not in user_query:
            valid_query = False
            print('\nQuery must include FROM statement\n')
        else:
            valid_query = True
    
    return user_query

def get_index_command():
    print()
    print('show indexes                       - Show all existing indexes')
    print('show indexes TABLE                 - Show all indexes for table')
    print('create keyword index TABLE KEYWORD - Create a new keyword index')
    print('delete index TABLE                 - Deletes all indexes on table')
    print()
    
    index_command = input('index > ')
    # TODO: validation of input?
    
    return index_command

def execute_user_command(user_command):
    
    # cmd is the map from user input to program function
    cmd = {
        'help':cmd_help,
        'quit':cmd_quit,
        'query':cmd_query,
        'index':cmd_index,
        'sort':cmd_sort,
        'show tables':cmd_show_tables,
        'show attributes in':cmd_show_attributes
    }
    
    if user_command.lower().startswith('show attributes in') == True:
        # get remainder of string after 'show attributes in'
        table_name = user_command[len('show attributes in'):len(user_command)]
        table_name = table_name.strip()
        user_command = 'show attributes in'
        return cmd[user_command](table_name)
    else:
        return cmd[user_command]()


def get_user_command():
    # Retrieve a command line input from user.
    
    user_command = input('\n> ')
    return user_command


# COMMAND METHODS


def cmd_help():
    # Show list of commands, and how to use them
    
    print('The following commands are available:')
    print('query . . . . . . . . . . . . Go to the query prompt to run a SQL query')
    print('show tables . . . . . . . . . Show the available CSV files for querying')
    print('show attributes in [TABLE]  . Show the available attributes in a given table.')
    print('quit  . . . . . . . . . . . . Quit the program')
    print('help  . . . . . . . . . . . . List functions available for use')
    print()
    print('Query format:')
    print('SELECT [attr] FROM [table] WHERE ([condition])')
    print('\nQUERY FORMATTING NOTES:')
    print('1) Multiple WHERE conditions can be separated by logical operators:')
    print('    WHERE (Year = 2000) AND (Value < 200)')
    print('2) Not equal to is handled by a <> operator')
    print()
    
    return True


def cmd_quit():
    # Do nothing. Return False. This will cause the main loop to exit.
    
    print('\n***Goodbye***\n')
    return False

def cmd_index():
    index_command_handler()
    return True
    
def cmd_sort():
    print('\nSort function not yet implemented\n')
    return True

def cmd_query():
    # Collect a query from the user.
    user_query = get_user_query()
    
    # If user_query is an empty string, don't do anything, return to main
    if user_query != '':
    
        # START TIMER - after receiving user query
        start_time = time.time()
        
        # Parse  Perform  Display results.
        input_query = Query(user_query)
        query_result_list = perform_query(input_query)
        display_query_result(query_result_list)
    
        # END TIMER - after displaying query
        print("--- %s seconds ---" % (time.time() - start_time))
    
    return True


def cmd_show_tables():
    # show list of .csv files in /tables, one per line
    
    table_list = get_table_list()
    print('\nList of available tables:')
    for table_name in table_list:
        print('  *', table_name)

    return True


def cmd_show_attributes(table_name):
    # For a given table, show a list of its attributes
    
    csv_filename = table_name + '.csv'
    csv_fullpath = os.path.join(get_table_directory(), csv_filename)

    attribute_list = get_attribute_list(csv_fullpath)

    print('\nAttributes in', table_name)
    for attribute in attribute_list:
        print('*', attribute)

    return True


# MAIN

if __name__ == '__main__':
    print('\n\n!!!SQL CSV ASAP!!!')
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

    # Remove temporary files
    if get_testmode() == False:
        remove_temp_files()