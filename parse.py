from utils import *

def get_sql_terms():
    # List of SQL terms handled by program
    return ['SELECT', 'FROM', 'WHERE']

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
    
def parse_select(raw_query, alias_list=[]):
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
    
    # Final step: replace all attribute names with table.name
    # TODO: include replacement of aliases
    for i in select_list:
        continue
    
    return select_list
    
def parse_from(raw_query, alias_list=[]):
    from_substring = find_query_substring(raw_query, 'FROM')
    return from_substring

def parse_where_with_alias(raw_query):
    
    return where_alias_substring
    
def parse_where(raw_query, alias_list=[]):
    where_substring = find_query_substring(raw_query, 'WHERE')
    return where_substring    
    
def parse_alias(raw_query):
    alias_dict = {}
    
    from_substring_with_alias = parse_where_with_alias(raw_query)
    
    return ''
    
class Query:
    def __init__(self, user_input):
        self.user_input = user_input
        self.alias = parse_alias(self.user_input)
        self.FROM = parse_from(self.user_input)
        self.SELECT = parse_select(self.user_input)
        self.WHERE = parse_where(self.user_input)
    
    def show_parsed_query(self):
        print('SELECT:', self.SELECT)
        print('FROM:', self.FROM)
        print('WHERE:', self.WHERE)
