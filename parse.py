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
        pass
    else:
        select_list = select_substring.split(',')
    
    # Get rid of extra spaces in list entries
    for i in range(len(select_list)):
        select_list[i] = select_list[i].strip()
    
    # Replace table alias with table names
    select_list = replace_table_alias(select_list, alias_dict)
    
    # Final step: replace all attribute names with table.name
    # TODO: include replacement of aliases
    for i in select_list:
        continue
    
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
    for i in range(len(from_alias_list)):
        from_alias_list[i] = from_alias_list[i].strip()
    
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
            
    # Final step: replace all attribute names with table.name
    # TODO: include replacement of aliases
    for i in where_list:
        continue
    
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

        self.attribute_dict = {}
    
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
                print(this_line)
