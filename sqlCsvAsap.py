import os

# CLASSES

class query:
    def __init__(self, qSelect, qFrom, qWhere):
        self.qSelect = qSelect
        self.qFrom = qFrom
        self.qWhere = qWhere

# METHODS

def getUserQuery():
    print('\nEnter SELECT-FROM-WHERE query:')
    valid = False
    while valid == False:
        queryInput = input('query > ')
        # parse queryInput into SELECT, FROM, WHERE, etc.
        # validate components
        valid = True #temporary to get out of validation loop

    #userQuery = query(inputSelect, inputFrom, inputWhere)
    userQuery = queryInput #temporary
    return userQuery

def validateUserCommand(userCommand):
    # TODO - check if it's in the cmd dictionary
    # TODO - maybe also handle the capitalization
    valid = True
    return valid

def executeUserCommand(userCommand):
    cmd = {
        'help':cmdHelp,
        'quit':cmdQuit,
        'query':cmdQuery,
        'show tables':cmdShowTables
        #TODO: show attributes in table - need some more processing for this one
    }
    #TODO: something to handle an invalid command
    
    result = True
    return cmd[userCommand]()

def getUserCommand():
    """
    Retrieve a command line command from user.
    Validate it, then pass it back.
    """
    valid = False
    while valid == False:
        userCommand = input('> ')
        valid = validateUserCommand(userCommand)
    return userCommand

# COMMAND METHODS

def cmdHelp():
    # Show list of commands, and how to use them
    print('TODO: description of available functions and how to use them')
    return True

def cmdQuit():
    # Do nothing. Return False. This will cause the main loop to exit.
    print('Goodbye.')
    return False

def cmdQuery():
    # This is the signal to the program to collect a query from the user.
    userQuery = getUserQuery()

    # TODO: after getting the query, parse it into its components (select, from, where, etc.)
    return True

def cmdShowTables():
    # show list of .csv files in /tables, one per line
    currentDirectory = os.getcwd()
    tableDirectory = currentDirectory + '\\tables'

    # I don't think this is the best way to do this, but it works for now...
    # os.walk through /tables directory and add files to filenames list,
    # then walk back through filenames list and throw out everything that isn't a .csv
    # TODO: Probably it's best to not add a non-csv to the list in the first place...
    
    # https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory#3207973
    f = []
    for (dirpath, dirnames, filenames) in os.walk(tableDirectory):
        f.extend(filenames)
        break

    for entry in filenames:
        if '.csv' not in entry:
            filenames.remove(entry)

    print('List of available tables:')
    for entry in filenames:
        # tableName is the filename minus .csv
        # For now, assuming that all entries end in .csv, although there should be a test for it
        tableName = entry[0:len(entry)-4]
        print('*', tableName)

    return True

# VALIDATION METHODS

def validateSelect(input):
    # Conditions for validity:
    # - Attribute must exist
    valid = False

    #TODO: hardcode True for now
    valid = True
    
    return valid

def validateFrom(input):
    # Conditions for validity:
    # - Table must exist
    valid = False

    #TODO: hardcode True for now
    valid = True
    
    return valid

def validateWhere(input):
    # Conditions for validity:
    # - Type must be correct
    valid = False

    #TODO: hardcode True for now
    valid = True
    return valid

def validQuery(validSelect, validFrom, validWhere):
    #TODO: should have some meaningful error messages
    valid = True
    if validSelect == False: valid = False
    if validFrom == False: valid = False
    if validWhere == False: valid = False

    return valid

# MAIN

if __name__ == '__main__':
    print('SQL CSV ASAP')

    # Keep running this loop while the user is using the program.
    # Only the command 'quit' should return False, thus exiting the loop.
    keepGoing = True
    while keepGoing == True:
        userCommand = getUserCommand()
        keepGoing = executeUserCommand(userCommand)
