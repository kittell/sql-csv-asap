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
    

def perform_query(query):
    # loop through tables (FROM)
    # test rows (WHERE)
    # print attribute values (SELECT)

    # Assumption: SELECT and FROM are valid
    csv_list = []
    for table in query['FROM']:
        csv_list.append(utils.table_to_csv(table))

    for csv_file in csv_list:
        csv_fullpath = utils.get_table_directory() + '//' + csv_file
        attribute_list = utils.get_attribute_list(csv_fullpath)
        with open(csv_fullpath, newline = '') as f:
            reader = csv.reader(f)
            for row in reader:
                # test each row against where criteria
                # 1) which attribute are we testing?
                # TODO: this is just comparing strings; fix that
                for i in range(len(query['WHERE'])):
                    for j in range(len(attribute_list)):
                        if query['WHERE'][i]['Subject'] == attribute_list[j]:
                            # here's the test
                            exp = query['WHERE'][i]['Subject']
                            op = query['WHERE'][i]['Verb']
                            val = query['WHERE'][i]['Object']
                            
                            #TODO: temp for developing - only does equality
                            if eval_binary_comparison(row[j], op, val):
                                print()
                                for x in range(len(query['SELECT'])):
                                    for y in range(len(attribute_list)):
                                        if query['SELECT'][x] == '*':
                                            print(row[y], end=',')
                                        elif query['SELECT'][x] == attribute_list[y]:
                                            print(row[y], end=',')
                                            break
                                #print(attribute, op, val)
                            break
