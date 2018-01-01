from utils import *
from index import *
import string

def get_sql_terms():
    # List of SQL terms handled by program
    return ['SELECT', 'FROM', 'WHERE', 'ORDER BY']

def get_sql_terms_nospace():
    # Replace the space with a dash. This is for parsing functions that are splitting
    # on space to not get confused...
    sql_terms = get_sql_terms()
    for i in range(len(sql_terms)):
        sql_terms[i] = sql_terms[i].replace(' ', '-')
    return sql_terms

def get_connector_list():
    # List of connectors for WHERE terms
    return ['AND', 'OR', 'NOT']
    
def get_verb_list():
    # List of relationships for WHERE terms
    # Order is kind of important - want NOT LIKE before LIKE
    return ['NOT LIKE', 'LIKE', '=', '<>', '<', '<=', '>', '>=']
    
def get_operator_list():
    # List of operators for WHERE terms
    return ['+', '-', '/', '*']

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
    
    sql_terms = get_sql_terms()
    sql_terms_nospace = get_sql_terms_nospace()
    
    for i in range(len(sql_terms)):
        if ' ' in sql_terms[i]:
            this_term = this_term.replace(sql_terms[i], sql_terms_nospace[i])
    
    sql_terms = get_sql_terms_nospace()
    broken_query = raw_query.split(' ')
    
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
            for term in sql_terms_nospace:
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
    DESCRIPTION: Replaces table_name with table_alias
    INPUT: 
        - original list: list of references with table names
        - alias_dict: map of alias:table
    OUTPUT: replaced_list: original list values with table_name replaced with table_alias
    """
    
    # TODO: Error if a problem with replacing alias
    
    replaced_list = []
    for original in original_list:
        found = False
        for table_alias in alias_dict:
            if original.startswith(alias_dict[table_alias] + '.') == True:
                temp = original.split('.')
                replace = combine_table_attribute_pair(table_alias, temp[1])
                replaced_list.append(replace)
                found = True
                break
        
        if found == False:
            # not in alias_dict, probably wasn't aliased so add it directly
            replaced_list.append(original)
    
    return replaced_list
    
    
class Query:
    """QUERY
    DESCRIPTION: The Query class holds the raw SQL user input and its parsed components.
    """

    def __init__(self, user_input):
        self.user_input = user_input
        self.prepare_user_input()
        self.parse_distinct()       # Run before parsing query - removes DISTINCT from query string
        
        # FROM goes first. It contains alias information used elsewhere in parsed query
        self.FROM = self.parse_from()   
        # dict that maps aliases in FROM clause to the actual table name     
        self.alias = self.parse_alias()
            
        self.table_list = self.get_raw_query_table_list()

        self.SELECT = self.parse_select()
        self.WHERE = self.parse_where()
        self.ORDERBY = self.parse_orderby()
        
        # join_constraints maps table_alias pairs to WHERE clause components
        self.join_constraints = self.map_join_constraints()
        # value_constraints maps table_aliases to WHERE clause components
        self.value_constraints = self.map_value_constraints()
        # where_table_attribute list is a list of table_aliases from WHERE clause
        self.where_table_attribute_list = self.get_where_table_attr_list()
        
        # dictionary with table_alias as key and a list of attribute names from the table
        #   as value
        self.attribute_dict = self.get_attribute_dict()
        
        # Print information about the query
        self.show_parsed_query()


    def prepare_user_input(self):
        # Combine SQL terms with more than one word into a single word
        sql_terms = get_sql_terms()
        sql_terms_nospace = get_sql_terms_nospace()
        
        self.prepared_user_input = self.user_input
        
        for i in range(len(sql_terms)):
            if ' ' in sql_terms[i]:
                self.prepared_user_input = self.prepared_user_input.replace(sql_terms[i], sql_terms_nospace[i])

        # Trim spaces on ends
        self.prepared_user_input = self.prepared_user_input.strip()


    def parse_distinct(self):
        self.distinct = False
        if ('SELECT DISTINCT') in self.prepared_user_input:
            self.distinct = True
            self.prepared_user_input = self.prepared_user_input.replace('SELECT DISTINCT', 'SELECT')

        
    def get_query_table_list(self):
        """GET_QUERY_TABLE_LIST
        DESCRIPTION: Build a list of all tables referenced in query SELECT and WHERE clauses
        INPUT: self
        OUTPUT: none (modify self.table_list)
        """
        self.table_list = []

        for table_name in self.value_constraints:
            if table_name not in self.table_list:
                self.table_list.append(table_name)
        
        for table_pair in self.join_constraints:
            for table_name in table_pair:
                if table_name not in self.table_list:
                    self.table_list.append(table_name)
        
        for table_attr in self.SELECT:
            (table_name, attr_name) = parse_table_attribute_pair(table_attr)
            if table_name not in self.table_list:
                self.table_list.append(table_name)


    def get_where_table_attr_list(self):
        """GET_WHERE_TABLE_ATTR_LIST
        DESCRIPTION: Get all table_alias-attribute pairs from WHERE clause. Table aliases
            are used instead of tables to account for self-joins
        INPUT: self
            - dict self.WHERE
            - list self.value_constraints
            - list self.join_constraints
        OUTPUT: list table_alias_list: list of table_alias-attribute pairs in WHERE clause
        """
        table_alias_list = []
        
        for table_alias in self.value_constraints:
            for i in self.value_constraints[table_alias]:
                if self.WHERE[i]['Subject'] not in table_alias_list:
                    table_alias_list.append(self.WHERE[i]['Subject'])
        
        for table_alias in self.join_constraints:
            for i in self.join_constraints[table_alias]:
                if self.WHERE[i]['Subject'] not in table_alias_list:
                    table_alias_list.append(self.WHERE[i]['Subject'])
                if self.WHERE[i]['Object'] not in table_alias_list:
                    table_alias_list.append(self.WHERE[i]['Object'])
        
        return table_alias_list
                    
    def show_parsed_query(self):
        """SHOW_PARSED_QUERY
        DESCRIPTION: Print information about the query to the console.
        INPUT: self
        OUTPUT: none (write to console)
        """
        select_statement = '\nSELECT: '
        if self.distinct == True:
            select_statement += 'DISTINCT '
        select_statement += ', '.join(self.SELECT)
        print(select_statement)
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
                if 'Operator' in self.WHERE[i] and 'Operand' in self.WHERE[i]:
                    this_line = this_line + ' ' + self.WHERE[i]['Operator'] + ' ' + str(self.WHERE[i]['Operator'])
                if self.WHERE[i]['Join'] == True:
                    this_line = this_line + ' (JOIN)'
                print(this_line)
        print('ORDER BY:', self.ORDERBY)

        test_print('prepared_user_input', self.prepared_user_input)
        test_print('table_list', self.table_list)
        
    def parse_select(self):
        """PARSE_SELECT
        DESCRIPTION: Retrieve the SELECT component of a SQL query
        INPUT: self.prepared_user_input
        OUTPUT: select_list: list of individual table.attributes to project in final results
        """
        select_substring = find_query_substring(self.prepared_user_input, 'SELECT')
    
        select_list = []
    
        if '*' in select_substring:
            # Special case: SELECT *
            # Stuff all attributes from table into select_list
            # Assumption: only one table involved - defined in FROM query
            from_query = self.parse_from()
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
        select_list = replace_table_alias(select_list, self.alias)
    
        # Final step: replace all attribute names with table.name
        select_list = self.force_table_attr_pairs(select_list)
    
        return select_list

    def parse_from_with_alias(self):
        """PARSE_FROM_WITH_ALIAS
        DESCRIPTION: Intermediate result for determing FROM query, returns partial FROM query
            that still contains aliases, if applicable
        INPUT: self.prepared_user_input
        OUTPUT: from_list: list of FROM terms with aliases (both in same list entry)
        """
        from_substring = find_query_substring(self.prepared_user_input, 'FROM')
        from_alias_list = from_substring.split(',')
    
        # Get rid of extra spaces in list entries
        # Also: remove .csv from end of table names, if it exists
        for i in range(len(from_alias_list)):
            from_alias_list[i] = from_alias_list[i].strip()
            
            # Remove .csv from table name, if it's there
            from_alias_list2 = from_alias_list[i].split(' ')
            for j in range(len(from_alias_list2)):
                if from_alias_list2[j].endswith('.csv'):
                    from_alias_list2[j] = from_alias_list2[j][:-4]
            from_alias_list[i] = ' '.join(from_alias_list2)
    
        return from_alias_list

    
    def parse_from(self):
        """PARSE_FROM
        DESCRIPTION: Retrieve the SELECT component of a SQL query
        INPUT: raw_query string input from user
        OUTPUT: from_list: list of FROM terms (without aliases)
        """
        from_alias_list = self.parse_from_with_alias()
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
        
    def parse_alias(self):
        """PARSE_ALIAS
        DESCRIPTION: Build a dictionary of table aliases, used for translating in query terms.
            Also include non-aliased tables; their key and value will be the same.
        INPUT: self.prepared_user_input
        OUTPUT: alias_dict: key: alias; value, actual table name
        """
        alias_dict = {}
        # get the intermediate from list that has the table and alias 
        from_alias_list = self.parse_from_with_alias()
    
        # Break each list entry by spaces - so 0 will be table (value), 1 will be alias (key)
        for table_alias in from_alias_list:
            if ' ' in table_alias:
                # If there's a space, then there's an alias - get first part of Table ALIAS
                from_alias = table_alias.split(' ')
                alias_dict[from_alias[1]] = from_alias[0]
            else:
                # If there isn't an alias, add table:table as a key:value pair
                alias_dict[table_alias] = table_alias
    
        return alias_dict

    
    def parse_where(self):
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
    
        where_substring = find_query_substring(self.prepared_user_input, 'WHERE')
    
        # If there is no WHERE, return empty list
        where_list = []
        if where_substring == '':
            return where_list
    
        initial_where_list = where_substring.split(' ')
    
        # Replace table alias with table names
        initial_where_list = replace_table_alias(initial_where_list, self.alias)
    
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
                # append on object term -- possible to search for string split by spaces
                # problem is the .split(' ') above would put it in two different list entries
                # which would overwrite here if you don't append
                if where_list[w]['Object'] == '':
                    where_list[w]['Object'] = initial_where_list[i]
                else:
                    # If Object isn't empty, and a math operator shows up -- special case
                    if initial_where_list[i] in get_operator_list():
                        where_list[w]['Operator'] = initial_where_list[i]
                    elif 'Operator' in where_list[w]:
                        # Operator term exists, expecting next term to be a number
                        # TODO: should check that, eh?
                        where_list[w]['Operand'] = initial_where_list[i]
                    else:
                        # Keep adding terms to Object
                        where_list[w]['Object'] = where_list[w]['Object'] + ' ' + initial_where_list[i]
            
        # Remove single-quotes from beginning and ending of WHERE Object terms
        for w in range(len(where_list)):
            where_list[w]['Object'] = where_list[w]['Object'].strip("'")
            
            
        # Replace all attribute names with table.attribute
        where_list = self.force_table_attr_pairs(where_list)
    
        # Determine whether WHERE clauses are joins
        for w in range(len(where_list)):
            where_list[w]['Join'] = check_has_join(where_list[w]['Subject'], where_list[w]['Object'])
    
        return where_list
    
    def parse_orderby(self):
        # TODO: doesn't work yet
        # TODO: currently only handles a single term, ascending sort
        orderby_substring = find_query_substring(self.prepared_user_input, 'ORDER BY')
        orderby_list = [orderby_substring]
    
        # Replace table alias with table names
        orderby_list = replace_table_alias(orderby_list, self.alias)
    
        # Replace all attribute names with table.attribute
        orderby_list = self.force_table_attr_pairs(orderby_list)

        # TODO: redo this when it's possible to sort on multiple terms
        orderby_dict = {}
        orderby_dict[orderby_substring] = 'ASC'
    
        return orderby_list
        
        
    def get_raw_query_table_list(self):
        """GET_RAW_QUERY_TABLE_LIST
        DESCRIPTION: Get a list of tables called out in a query. Don't confuse with building
            a list of tables available to query (i.e., in /tables folder)
        INPUT: self.prepared_user_input
        OUTPUT: query_table_list: list of tables called out in query
        DEPENDENCY: string.punctuation
        """
        query_table_list = []
        full_table_list = get_table_list()

        # Remove punctuation from string
        # https://stackoverflow.com/a/34294398/752784
        # remove_this is string.punctuation, minus the -
        remove_this = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
        translator = str.maketrans('', '', remove_this)
        query_string = self.prepared_user_input.translate(translator)

        # Break raw query up so it's just a list of terms        
        broken_query = query_string.split(' ')
    
        # Loop over broken_query, finding terms that match full_table_list
        for term in broken_query:
            for table_name in full_table_list:
                if term == table_name:
                    query_table_list.append(table_name)
                    break
    
        return query_table_list
        
    def force_table_attr_pairs(self, input_list):
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
    
        attribute_dict = self.get_attribute_dict()
    
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

    def map_join_constraints(self):
        """MAP_JOIN_CONSTRAINTS
            DESCRIPTION: Maps WHERE clauses to joined table pairs
            INPUT:
                - query_where: WHERE component of parsed SQL query
            OUTPUT: join_constraints: dict of list; key of dict is a tuple of (table1, table2);
                value of dict is a list of index numbers from main WHERE query containing 
                constraint relevant to this table-table pair
                e.g., join_constraints[(table_alias1, table_alias2)] = 0
                      join_constraints[(table_alias2, table_alias3)] = 1
            DEPENDENCY: utils.parse_table_attribute_pair
        """
        join_constraints = {}
        for i in range(len(self.WHERE)):
            if self.WHERE[i]['Join'] == True:
                [table_alias1, attr_name1] = parse_table_attribute_pair(self.WHERE[i]['Subject'])
                [table_alias2, attr_name2] = parse_table_attribute_pair(self.WHERE[i]['Object'])
                table_alias_pair = (table_alias1, table_alias2)
                if table_alias_pair not in join_constraints:
                    # Create empty list for key not already in join_constraints
                    join_constraints[table_alias_pair] = []
                join_constraints[table_alias_pair].append(i)
        test_print('map_join_constraints / join_constraints', join_constraints)
        return join_constraints


    def map_value_constraints(self):
        """MAP_VALUE_CONSTRAINTS
            DESCRIPTION: Maps WHERE clauses to value constraints
            INPUT:
                - query_where: WHERE component of parsed SQL query
            OUTPUT: value_constraints: dict of lists; key of dict is the table that the list of
                requirements applies to; value of dict is a list of index numbers from the main
                WHERE query containing a constraint relevant to this table
                e.g., value_constraints[table_alias1] = 0
        """
        value_constraints = {}
    
        for i in range(len(self.WHERE)):
            if self.WHERE[i]['Join'] == False:
                [table_alias, attr_name] = parse_table_attribute_pair(self.WHERE[i]['Subject'])
                if table_alias not in value_constraints:
                    # Create empty list for key not already in value_constraints
                    value_constraints[table_alias] = []
                value_constraints[table_alias].append(i)
            
        test_print('map_value_constraints / value_constraints', value_constraints)
        return value_constraints
        
    def get_attribute_dict(self):
        """GET_ATTRIBUTE_DICT
            DESCRIPTION: Build a dictionary with table_alias as key and a list of attribute
                for that table as values.
            INPUT: self
                - dict self.alias
            OUTPUT: dict attribute_dict: dictionary that maps table_alias to list of attribute
                names, e.g., attribute_dict[table_alias] = [attr_name1, attr_name2]
        """
        # Loop through table list
        # Build attribute_list for each table
        # This function is used in two places:
        #  1) indexing - where there is no alias dict available yet
        #  2) parsing query
    
        attribute_dict = {}
        for table_alias in self.alias:
            csv_fullpath = table_to_csv_fullpath(self.alias[table_alias])
            attribute_list = get_attribute_list(csv_fullpath)
            attribute_dict[table_alias] = attribute_list
        
        return attribute_dict
        
    def get_attribute_dict_index(self, table_alias_attr):
        """GET_ATTRIBUTE_DICT_INDEX
            DESCRIPTION: Gets position of an attr_name field in a given table_alias
            INPUT: self
                - dict self.alias
                - table_alias_attr: table alias name and attribute name pair
                    in one of two forms: (1) string, table_alias.attr_name; 
                    (2) list, [table_alias, attr_name]
            OUTPUT: dict attribute_dict: dictionary that maps table_alias to list of attribute
                names, e.g., attribute_dict[table_alias] = [attr_name1, attr_name2]
        """
        
        # Case 1: combined: table_alias_name.attr_name
        # Case 2: split [table_alias_name, attr_name]
    
        if '.' in table_alias_attr:
            # Case 1: table_alias_attr = table.attribute pair - split it out
            [table_alias, attr_name] = parse_table_attribute_pair(table_alias_attr)
        else:
            # Case 2: table_alias_attr already split into [table_name, attr_name]
            [table_alias, attr_name] = table_alias_attr
        
        for a in self.alias:
            if a == table_alias:
                # Found table_alias in alias dict -- now find attr_name in list
                for i in range(len(self.attribute_dict[table_alias])):
                    if self.attribute_dict[table_alias][i] == attr_name:
                        #TODO: This assumes table and attr are found in alias dict
                        return i