# CLASSES

class query:
    def __init__(self, qSelect, qFrom, qWhere):
        self.qSelect = qSelect
        self.qFrom = qFrom
        self.qWhere = qWhere

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

# METHODS

def getTableList():
    #TODO: hardcode this for now; later, search for tables
    tableList = ['BostonMarathon2017.csv']
    return tableList

def getAttributeList(table):
    #TODO: hardcode this for now; later, search for for attributes in table
    return attributeList

def getUserQuery():
    print('\nEnter SELECT-FROM-WHERE query:')
    valid = False
    while valid == False:
        inputSelect = input('SELECT: ')
        inputFrom = input('FROM: ')
        inputWhere = input('WHERE: ')

        validSelect = validateSelect(inputSelect)
        validFrom = validateFrom(inputFrom)
        validWhere = validateWhere(inputWhere)
        valid = validQuery(validSelect, validFrom, validWhere)

    userQuery = query(inputSelect, inputFrom, inputWhere)
    return userQuery


# MAIN

# Tell user what tables and attributes are available
print('Available tables and attributes:')
#TBD
print('TBD')

# Ask user for query

userQuery = getUserQuery()
