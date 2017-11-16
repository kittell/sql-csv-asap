import utils
import operator
import csv

def get_comparison_function(c):
    #inspiration: https://stackoverflow.com/a/1740759/752784
    # operator library: https://docs.python.org/3/library/operator.html
    return {
            '=': operator.eq,
            '<': operator.lt,
            '<=': operator.le,
            '<>': operator.ne,
            '>': operator.gt,
            '>=': operator.ge
        }[c]

        
def eval_binary_comparison(a, op, b):
    return get_comparison_function(op)(a, b)


def get_select_values(row, query_select, attribute_list):
    select_list = []
    
    for x in range(len(query_select)):
        for y in range(len(attribute_list)):
            if query_select[x] == '*':
                select_list.append(row[y])
            elif query_select[x] == attribute_list[y]:
                select_list.append(row[y])
                break
    return select_list


def test_row(row, query_where, attribute_list):
    """
    INPUT:
        - row to test (list)
        - where criteria or none
    OUTPUT: true/pass; false
    """
    result = False
    
    if query_where == '':
        # If WHERE wasn't specified, don't test the row, just pass it
        result = True
    else:
        for i in range(len(query_where)):
            for j in range(len(attribute_list)):
                if query_where[i]['Subject'] == attribute_list[j]:
                    if eval_binary_comparison(row[j], query_where[i]['Verb'], query_where[i]['Object']):
                        result = True
                        break
    return result

def perform_query(query):
    # Assumption: SELECT and FROM are valid
    
    # Get list of csv files from FROM query.
    csv_list = []
    for table in query['FROM']:
        csv_list.append(utils.table_to_csv(table))

    result_list = []
    for csv_file in csv_list:
        csv_fullpath = utils.get_table_directory() + '//' + csv_file
        attribute_list = utils.get_attribute_list(csv_fullpath)
        
        with open(csv_fullpath, newline = '') as f:
            reader = csv.reader(f)
            for row in reader:
                take_row = test_row(row, query['WHERE'], attribute_list)
                if take_row == True:
                    result_list.append(get_select_values(row, query['SELECT'], attribute_list))
    
    return result_list