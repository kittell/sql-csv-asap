from utils import *

def get_sql_terms():
    # List of SQL terms handled by program
    return ['SELECT', 'FROM', 'WHERE']


def get_connector_list():
    # List of connectors for WHERE terms
    return ['AND', 'OR', 'NOT']
    
def get_verb_list():
    # List of operators
    # Order is kind of important - want NOT LIKE before LIKE
    return ['NOT LIKE', 'LIKE', '=', '<>', '<', '<=', '>', '>=']

def map_join_constraints(q):
    """MAP_JOIN_CONSTRAINTS
        DESCRIPTION: Maps WHERE clauses to joined table pairs
        INPUT:
            - query_where: WHERE component of parsed SQL query
        OUTPUT: join_constraints: dict of list; key of dict is a tuple of (table1, table2);
            value of dict is a list of index numbers from main WHERE query containing 
            constraint relevant to this table-table pair
            e.g., join_constraints[(table1, table2)] = 0
                  join_constraints[(table2, table3)] = 1
        DEPENDENCY: utils.parse_table_attribute_pair
    """
    join_constraints = {}
    for i in range(len(q.WHERE)):
        if q.WHERE[i]['Join'] == True:
            ta1 = parse_table_attribute_pair(q.WHERE[i]['Subject'])
            ta2 = parse_table_attribute_pair(q.WHERE[i]['Object'])
            table_pair = (ta1[0], ta2[0])
            if table_pair not in join_constraints:
                # Create empty list for key not already in join_constraints
                join_constraints[table_pair] = []
            join_constraints[table_pair].append(i)
    test_print('map_join_constraints / join_constraints', join_constraints)
    return join_constraints


def map_value_constraints(q):
    """MAP_VALUE_CONSTRAINTS
        DESCRIPTION: Maps WHERE clauses to value constraints
        INPUT:
            - query_where: WHERE component of parsed SQL query
        OUTPUT: value_constraints: dict of lists; key of dict is the table that the list of
            requirements applies to; value of dict is a list of index numbers from the main
            WHERE query containing a constraint relevant to this table
    """
    value_constraints = {}
    
    for i in range(len(q.WHERE)):
        if q.WHERE[i]['Join'] == False:
            ta1 = parse_table_attribute_pair(q.WHERE[i]['Subject'])
            table1 = ta1[0]      # just parsing to make code more understandable
            if table1 not in value_constraints:
                # Create empty list for key not already in value_constraints
                value_constraints[table1] = []
            value_constraints[table1].append(i)
            
    test_print('map_value_constraints / value_constraints', value_constraints)
    return value_constraints

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
    
    result = False
    has_dot = False
    split_not_number = False
    table_list = []
    
    # Test 1: Subject and Object have a dot
    # Test 2: not numbers on both sides of dot
    if '.' in subject:
        if '.' in object:
            has_dot = True
            split_not_number = True
                        
            subject_split = subject.split('.')
            for j in subject_split:
                if j.isnumeric() == True:
                    split_not_number = False
            
            object_split = object.split('.')
            for j in object_split:
                if j.isnumeric() == True:
                    split_not_number = False
            
    if has_dot == True and split_not_number == True:
        result = True
    
    return result
    
    
def find_query_substring(raw_query, this_term):
    """FIND_QUERY_SUBSTRING
    DESCRIPTION: Find the component of a SQL term for this_term, e.g., SELECT
    INPUT: raw_query string input from user
    OUTPUT: result: substring of SQL query corresponding to this_term
    """
    # find the substring between this_term and the next sql term in raw_query
    # Split the whole raw_query into a list, separated by spaces
    # Find the entry for this_term and next_term
    # Reconstitute the substring from this_term to next_term - 1
    
    broken_query = raw_query.split(' ')
    term_list = get_sql_terms()
    
    i_this = 0
    i_next = 0
    
    # First: Find the index for this_term: i_this
    found = False
    for i in range(len(broken_query)):
        if found == False:
            if broken_query[i] == this_term:
                i_this = i
                found = True
                break
    
    # if term wasn't found, return empty string
    if found == False:
        return ''
    
    # Next: Find index for next_term: i_next
    i_next = len(broken_query) # default set to end in case it's last term in query
    found = False
    for i in range(i_this + 1, len(broken_query)):
        if found == False:
            for term in term_list:
                if term == broken_query[i]:
                    i_next = i
                    found = True
                    break

    
    # Reconstitute substring; go ahead and leave out the entry for the term
    # i.e., don't return SELECT for the SELECT substring
    result = ' '.join(broken_query[i_this + 1:i_next])
    return result
    
def replace_table_alias(original_list, alias_dict):
    """REPLACE_TABLE_ALIAS
    DESCRIPTION: Replaces table alias with the table name
    INPUT: 
        - original list: list with table aliases
        - alias_dict: map of alias:table
    OUTPUT: replaced_list: original list values with aliases replaced with table names
    """
    
    # TODO: Error if a problem with replacing alias
    
    replaced_list = []
    for original in original_list:
        found = False
        for alias in alias_dict:
            if original.startswith(alias + '.') == True:
                # Replace the alias with the actual table name
                temp = original.split('.')
                replace = alias_dict[alias] + '.' + temp[1]
                replaced_list.append(replace)
                found = True
                break
        
        if found == False:
            # not in alias_dict, probably wasn't aliased so add it directly
            replaced_list.append(original)
    
    return replaced_list
    
def force_table_attr_pairs(input_list, table_list):
    """FORCE_TABLE_ATTR_PAIRS
    DESCRIPTION: For a given list, append table name to attributes
    INPUT: 
        - input_list: list of terms to modify
        - table_list: list of tables available to append to attributes
    OUTPUT: mod_list: modification of input_list - table names appended to attributes
    """
    
    # Two cases to replace:
    # 1) Replace in dict in list, as in WHERE clause
    # 2) Replace in simple list, as in SELECT clause
    
    attribute_dict = get_attribute_dict2(table_list)
    
    # Loop through input_list
    for i in range(len(input_list)):
        if type(input_list[i]) is dict:
            # Case 1: WHERE clause
            # Loop through dict
            for k in input_list[i]:
                if '.' not in input_list[i][k]:
                    # If there is a . in dict entry, assume that means it already has a table.attr pair
                    # Loop through attribute_dict to find matching attribute
                    for t in attribute_dict:
                        for a in attribute_dict[t]:
                            if input_list[i][k] == a:
                                # Found matching attribute, append table_name
                                input_list[i][k] = t + '.' + input_list[i][k]
                                break
        else:
            # Other cases: just a list, as in SELECT
            for t in attribute_dict:
                for a in attribute_dict[t]:
                    if input_list[i] == a:
                        # Found matching attribute, append table_name
                        input_list[i] = t + '.' + input_list[i]
                        break
    
    return input_list
    
    
def parse_select(raw_query, alias_dict={}):
    """PARSE_SELECT
    DESCRIPTION: Retrieve the SELECT component of a SQL query
    INPUT: raw_query string input from user
    OUTPUT: select_list: list of individual table.attributes to project in final results
    """
    select_substring = find_query_substring(raw_query, 'SELECT')
    
    select_list = []
    
    if '*' in select_substring:
        # Special case: SELECT *
        # Stuff all attributes from table into select_list
        # Assumption: only one table involved - defined in FROM query
        from_query = parse_from(raw_query)
        table_name = from_query[0]
        
        # Get attribute list for this table
        csv_fullpath = table_to_csv_fullpath(table_name)
        select_list = get_attribute_list(csv_fullpath)
        
    else:
        select_list = select_substring.split(',')
    
    # Get rid of extra spaces in list entries
    for i in range(len(select_list)):
        select_list[i] = select_list[i].strip()
    
    # Replace table alias with table names
    if len(alias_dict) > 0:
        select_list = replace_table_alias(select_list, alias_dict)
    
    # Final step: replace all attribute names with table.name
    table_list = get_query_table_list(raw_query)
    select_list = force_table_attr_pairs(select_list, table_list)
    
    return select_list

def parse_from_with_alias(raw_query):
    """PARSE_FROM_WITH_ALIAS
    DESCRIPTION: Intermediate result for determing FROM query, returns partial FROM query
        that still contains aliases, if applicable
    INPUT: raw_query string input from user
    OUTPUT: from_list: list of FROM terms with aliases
    """
    from_substring = find_query_substring(raw_query, 'FROM')
    from_alias_list = from_substring.split(',')
    
    # Get rid of extra spaces in list entries
    # Also: remove .csv from end of table names, if it exists
    for i in range(len(from_alias_list)):
        from_alias_list[i] = from_alias_list[i].strip()
        if from_alias_list[i].endswith('.csv'):
            from_alias_list[i] = from_alias_list[i][:-4]
    
    return from_alias_list

    
def parse_from(raw_query):
    """PARSE_FROM
    DESCRIPTION: Retrieve the SELECT component of a SQL query
    INPUT: raw_query string input from user
    OUTPUT: from_list: list of FROM terms (without aliases)
    """
    from_alias_list = parse_from_with_alias(raw_query)
    from_list = []
    
    # Break each list entry by spaces - so 0 will be table (value), 1 will be alias (key)
    for i in from_alias_list:
        if ' ' in i:
            # If there's a space, then there's an alias - get first part of Table ALIAS
            from_alias = i.split(' ')
            from_list.append(from_alias[0])
        else:
            # Otherwise there is no alias, just append the table
            from_list.append(i)
    
    return from_list

    
def parse_where(raw_query, alias_dict={}):
    """PARSE_WHERE
    DESCRIPTION: Retrieve the WHERE component of a SQL query
    INPUT: candidate WHERE clause of input SQL query
    OUTPUT: parsed_list: list of individual components of WHERE clause
    """
    
    # WHERE is a list of dictionaries with components: connector, subject, verb, object
    # example: 
    #           q.WHERE[0]['Connector'] = 'AND'
    #           q.WHERE[0]['Subject'] = 'Name'
    #           q.WHERE[0]['Verb'] = '='
    #           q.WHERE[0]['Object'] = 'Bill'
    #           q.WHERE[0]['Join'] = True
    
    where_substring = find_query_substring(raw_query, 'WHERE')
    
    # If there is no WHERE, return empty list
    where_list = []
    if where_substring == '':
        return where_list
    
    initial_where_list = where_substring.split(' ')
    
    # Replace table alias with table names
    initial_where_list = replace_table_alias(initial_where_list, alias_dict)
    
    # Parse into individual dict terms for each individual WHERE clause
    connector_list = get_connector_list()
    verb_list = get_verb_list()
    w = 0       # WHERE counter
    first = True
    for i in range(len(initial_where_list)):
        # TODO: This sequence seems brittle -- i.e., will have problems if 
        # user inputs aren't Just Right.

        # For first term, there is no connector; initialize the whole dict
        if w == 0 and first == True:
            where_list.append({})
            where_list[w]['Connector'] = ''
            where_list[w]['Subject'] = ''
            where_list[w]['Verb'] = ''
            where_list[w]['Object'] = ''
            first = False
        
        # Starting a new clause if there's a connector
        if initial_where_list[i] in connector_list:
            # Special case: NOT LIKE
            if initial_where_list[i+1] == 'LIKE':
                continue
            else:
                w = w + 1
                where_list.append({})
                where_list[w]['Connector'] = initial_where_list[i]
                where_list[w]['Subject'] = ''
                where_list[w]['Verb'] = ''
                where_list[w]['Object'] = ''

        elif where_list[w]['Subject'] == '':
            where_list[w]['Subject'] = initial_where_list[i]
        elif where_list[w]['Verb'] == '':
            if initial_where_list[i] in verb_list:
                where_list[w]['Verb'] = initial_where_list[i]

                # Special case: NOT LIKE
                if initial_where_list[i] == 'LIKE':
                    if initial_where_list[i-1] == 'NOT':
                        where_list[w]['Verb'] = 'NOT LIKE'

            
            # Special case: NOT LIKE
            elif initial_where_list[i] == 'NOT':
                # Add it on the next loop
                continue

        else:
            where_list[w]['Object'] = initial_where_list[i]
            
    # Replace all attribute names with table.attribute
    table_list = get_query_table_list(raw_query)
    where_list = force_table_attr_pairs(where_list, table_list)
    
    # Determine whether WHERE clauses are joins
    for w in range(len(where_list)):
        where_list[w]['Join'] = check_has_join(where_list[w]['Subject'], where_list[w]['Object'])
    
    return where_list
    
def parse_alias(raw_query):
    """PARSE_ALIAS
    DESCRIPTION: Build a dictionary of table aliases, used for translating in query terms
    INPUT: raw_query string input from user
    OUTPUT: alias_dict: key: alias; value, actual table name
    """
    alias_dict = {}
    # get the intermediate from list that has the table and alias 
    from_alias_list = parse_from_with_alias(raw_query)
    
    # Break each list entry by spaces - so 0 will be table (value), 1 will be alias (key)
    for i in from_alias_list:
        if ' ' in i:
            # If there's a space, then there's an alias - get first part of Table ALIAS
            from_alias = i.split(' ')
            alias_dict[from_alias[1]] = from_alias[0]
    
    return alias_dict
    
class Query:
    def __init__(self, user_input):
        self.user_input = user_input
        self.alias = parse_alias(self.user_input)
        self.FROM = parse_from(self.user_input)
        self.SELECT = parse_select(self.user_input, self.alias)
        self.WHERE = parse_where(self.user_input, self.alias)
        
        self.query_table_list = get_query_table_list(self.user_input)
        self.attribute_dict = get_attribute_dict2(self.query_table_list)

        self.join_constraints = map_join_constraints(self)
        self.value_constraints = map_value_constraints(self)
        
        self.show_parsed_query()
    
    def show_parsed_query(self):
        print('SELECT:', self.SELECT)
        print('FROM:', self.FROM)
        if self.WHERE != '':
            print('WHERE:')
            for i in range(len(self.WHERE)):
                this_line = '    '
                this_line = this_line + str(i) + ': '
                if i > 0:
                    this_line = this_line + self.WHERE[i]['Connector'] + ' '
                this_line = this_line + self.WHERE[i]['Subject'] + ' '
                this_line = this_line + self.WHERE[i]['Verb'] + ' '
                this_line = this_line + self.WHERE[i]['Object']
                if self.WHERE[i]['Join'] == True:
                    this_line = this_line + ' (join)'
                print(this_line)
