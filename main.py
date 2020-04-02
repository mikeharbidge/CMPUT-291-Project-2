import re
from datetime import date
import datetime
from bsddb3 import db
import time

COMPARISON_SYMBOLS = {'>', '>=', '<', '<=', '='}
KEYWORDS = ['pterm', 'rterm', 'price', 'date', 'score']


def iterate_query(tokenized_query, query_dict):
    for keyword in KEYWORDS:
        if keyword in tokenized_query:
            index = tokenized_query.index(keyword)
            i = index+1
            while i < len(tokenized_query):
                if tokenized_query[i] in KEYWORDS or (i-index > 2 and tokenized_query[index] not in {"rterm", "pterm"}):
                    break
                else:
                    i += 1
            query_dict[keyword].append(
                [tokenized_query[index+1], ' '.join(tokenized_query[index+2:i])])
            del tokenized_query[index:i]


def evaluate_query(query):
    query_dict = {'pterm': [],
                  'rterm': [],
                  'price': [],
                  'date': [],
                  'score': [],
                  }
    query = re.sub(':', ' : ', query)
    query = re.sub('>', ' > ', query)
    query = re.sub('<', ' < ', query)
    query = re.sub('=', ' = ', query)
    query = re.sub('\s*%\s*', '% ', query)
    query = re.sub('\s*/\s*', '/', query)
    query = re.sub('>\s*=', ' >= ', query)
    query = re.sub('<\s*=', ' <= ', query)
    query = re.sub('\s+', ' ', query)
    query = query.lower().strip()
    tokenized_query = query.split(' ')
    iterate_query(tokenized_query, query_dict)
    iterate_query(tokenized_query, query_dict)
    if len(tokenized_query) > 0:
        query_dict['pterm'] = [":", ' '.join(tokenized_query)]
        query_dict['rterm'] = query_dict['pterm']
    for entry in query_dict['date']:
        date_list = entry[1].split('/')
        entry[1] = date(int(date_list[0]), int(
            date_list[1]), int(date_list[2]))
    return query_dict

def compare (lOperand, rOperand, operator):
    if operator == '>':
        return lOperand > rOperand
    if operator == '<':
        return lOperand < rOperand
    if operator == '=':
        return lOperand == rOperand
    if operator == '>=':
        return lOperand >= rOperand
    if operator == '<=':
        return lOperand <= rOperand

"""
Runs the query on the database, finds the results and prints them line by line. The output details must be based on full_output.
@param {dictionary} tokenized_query - A dictionary containing keys = {'pterm', 'rterm', 'date', 'price', 'score'} with their values stored as 
a tuple (operator, value). E.g. for this query: guitar date > 2007/05/16  price > 200 price < 300,
@param {boolean} full_output - Whether the results should be shown in the full format or the brief format
{
    'pterm': (':', 'guitar'), 
    'rterm': (':', 'guitar'), 
    'price': [('>', '200'), ('<', '300')], 
    'date': [('>', '2007/05/16')], 
    'score': []
}
"""


def results(tokenized_query, full_output):
    #reviews batabase open
    rw = db.DB() #handle for Berkeley DB database
    DB_File = "rw.idx"
    rw.open(DB_File ,None, db.DB_HASH, db.DB_CREATE)
    rw_curs = rw.cursor()

    #scores database open
    sc = db.DB() #handle for Berkeley DB database
    DB_File = "sc.idx"
    sc.open(DB_File ,None, db.DB_BTREE, db.DB_CREATE)
    sc_curs = sc.cursor()

    #pterms database open
    pt = db.DB() #handle for Berkeley DB database
    DB_File = "pt.idx"
    pt.open(DB_File ,None, db.DB_BTREE, db.DB_CREATE)
    pt_curs = pt.cursor()

    #rterms databse open
    rt = db.DB() #handle for Berkeley DB database
    DB_File = "rt.idx"
    rt.open(DB_File ,None, db.DB_BTREE, db.DB_CREATE)
    rt_curs = rt.cursor()
    #end of open
    
    #rterm filtering here
    #---------------------------------------------
    rt_result_set={None}
    rt_result_set.clear()
    rt_iter = rt_curs.first()
    if tokenized_query['rterm']:
        prefix = False
        if tokenized_query['rterm'][1].endswith('%'):
            prefix = True
            term = tokenized_query['rterm'][1].replace('%','')
        while rt_iter:
            result = rt.get(rt_iter[0])
            result = result.decode("utf-8")     #instead of encoding the token in the query the result just needs to be decoded
            print(result)   #debug
            print(rt_iter[0])
            if prefix:
                if rt_iter[0].decode("utf-8").startswith(term): #prints if rterm is in data, fixed
                    rt_result_set.add(result)
            else:
                if (str(tokenized_query['rterm'][1])) == rt_iter[0].decode("utf-8"): #prints if rterm is in data, fixed
                    rt_result_set.add(result)
            rt_iter = rt_curs.next()

    '''OLD way of rterm filtering that doesnt work
    i = 1
    rt_iter = rt_curs.first()
    if tokenized_query['rterm']:    #typo? i changed from pterm to rterm
        while rt_iter:
            result = rt.get(rt_iter[0])
            while i < len(tokenized_query['rterm']):
                print(tokenized_query['rterm'][i])
                if result.find(tokenized_query['rterm'][i].encode("utf-8")): #prints if rterm is in data
                    print(result)
                i = i + 2
            rt_iter = rt_curs.next()'''

    #pterm filtering here, modified from 
    #---------------------------------------------
    pt_result_set={None}
    pt_result_set.clear()
    pt_iter = pt_curs.first()
    if tokenized_query['pterm']:
        prefix = False
        if tokenized_query['pterm'][1].endswith('%'):
            prefix = True
            term = tokenized_query['pterm'][1].replace('%','')
        while pt_iter:
            result = pt.get(pt_iter[0])
            result = result.decode("utf-8")     #instead of encoding the token in the query the result just needs to be decoded
            print(result)   #debug
            print(pt_iter[0])
            if prefix:
                if pt_iter[0].decode("utf-8").startswith(term): #prints if rterm is in data, fixed
                    pt_result_set.add(result)
            else:
                if (str(tokenized_query['pterm'][1])) == pt_iter[0].decode("utf-8"): #prints if rterm is in data, fixed
                    pt_result_set.add(result)
            pt_iter = pt_curs.next()
    
    #scores filtering here
    #---------------------------------------------
    sc_result_set={None}
    sc_result_set.clear()
    sc_iter = sc_curs.first()
    if tokenized_query['score']:
        while sc_iter:
            result = sc.get(sc_iter[0])
            result = result.decode("utf-8")     #instead of encoding the token in the query the result just needs to be decoded
            print(result)   #debug
            print(sc_iter[0])
            if len(tokenized_query['score']) == 1:
                operator1 = tokenized_query['score'][0][0]
                operand1 = float(tokenized_query['score'][0][1])
                if compare(float(sc_iter[0].decode("utf-8")), operand1, operator1):
                    sc_result_set.add(result)
            elif len(tokenized_query['score']) == 2:
                operator1 = tokenized_query['score'][0][0]
                operator2 = tokenized_query['score'][1][0]
                operand1 = float(tokenized_query['score'][0][1])
                operand2 = float(tokenized_query['score'][1][1])
                if compare(float(sc_iter[0].decode("utf-8")), operand1, operator1) and compare(float(sc_iter[0].decode("utf-8")), operand2, operator2):
                    sc_result_set.add(result)
            sc_iter = sc_curs.next()
    
    combined_set = pt_result_set
    if len(rt_result_set) != 0:
        combined_set = combined_set.intersection(rt_result_set)
    if len(sc_result_set) != 0:
        combined_set = combined_set.intersection(sc_result_set)
    result_set = {None}
    result_set.clear()
    #date filtering here
    #---------------------------------------------
    rw_iter = rw_curs.first()
    while rw_iter:
        result = rw.get(rw_iter[0])
        result = result.decode("utf-8")     #instead of encoding the token in the query the result just needs to be decoded
        result = result.split(",")
        print(result)   #debug
        print(rw_iter[0])
        if rw_iter[0].decode('utf-8') in combined_set:
            if len(tokenized_query['date']) == 1:
                operator1 = tokenized_query['date'][0][0]
                operand1 = datetime.timestamp(tokenized_query['date'][0][1])
                if compare(float(result[7]), operand1, operator1):
                    result_set.add([rw_iter[0].decode('utf-8'), result])
        elif len(tokenized_query['date']) == 2:
            operator1 = tokenized_query['date'][0][0]
            operator2 = tokenized_query['date'][1][0]
            operand1 = datetime.timesmap(tokenized_query['date'][0][1])
            operand2 = datetime.timestamp(tokenized_query['date'][1][1])
            if compare(float(result[7]), operand1, operator1) and compare(float(result[7]), operand2, operator2):
                result_set.add([rw_iter[0].decode('utf-8'), result])
        rw_iter = rw_curs.next()

    #print out filtered results
    #---------------------------------------------
    """iter = rw_curs.first()
    while iter:
        result = rw.get(iter[0])
        hi = result.find(tokenized_query['pterm'][1].encode("utf-8"))
        if full_output and hi > 0:
            show_extended_results(result)
        elif hi > 0:
            show_brief_results(result)
        iter = rw_curs.next()
    #print(tokenized_query)
"""
    #closing databases
    #---------------------------------------------
    rw_curs.close()
    rw.close()
    rt_curs.close()
    rt.close()
    pt_curs.close()
    pt.close()
    sc_curs.close()
    sc.close()
    return result_set
        

        
    

"""
Prints the summerized version of a result entry (the review id, the product title and the review score of all matching reviews)
@param result - a result entry returned by the query cursor
"""


def show_brief_results(result_set):
    review_id = result_set[0]
    product_id = result_set[1][0]
    product_title = result_set[1][1]
    product_price = result_set[1][2]
    userid = result_set[1][3]
    profile_name = result_set[1][4]
    helpfulness = result_set[1][5]
    score = result_set[1][6]
    review_date = datetime.fromtimestamp(float(result_set[1][7]))
    summary = result_set[1][8]
    review_text = ','.join(result_set[1][9:])
    print("Review ID:", review_id, 
          "\nProduct:", product_title,
          "\nScore:", score)
                                
    


"""
Prints a result entry with additional details (all review fields)
@param result - a result entry returned by the query cursor
"""


def show_extended_results(result_set):
    # quotes = 0
    # result = result.decode("utf-8")
    # i=0
    # while i < len(result)-1:
    #     if result[i] == '"':
    #         quotes = quotes + 1
    #     if result[i] != ',':
    #         print(result[i], end = '')
    #     elif result[i] == ',' and quotes % 2 == 0:
    #         print()
    #     i = i + 1
    # print()
    # print()
    review_id = result_set[0]
    product_id = result_set[1][0]
    product_title = result_set[1][1]
    product_price = result_set[1][2]
    userid = result_set[1][3]
    profile_name = result_set[1][4]
    helpfulness = result_set[1][5]
    score = result_set[1][6]
    review_date = datetime.fromtimestamp(float(result_set[1][7]))
    summary = result_set[1][8]
    review_text = ','.join(result_set[1][9:])
    print("Review ID:", review_id, 
          "\nProduct ID:", product_id, "\tProduct:", product_title,"\tPrice:",product_price,
          "\nUser ID:", userid, "\tProfile:", profile_name,
          "\nHelpfulness:",helpfulness, "\tScore:", score, "\tReview date:", review_date,
          "\nSummary:", summary,
          "\n" + review_text)

if __name__ == "__main__":
    running = True
    full = False
    while (running):
        command = input("q to quit\n")
        #print(command)
        if re.compile('output\s*=\s*full').match(command.lower().strip()):
            full = True
        elif re.compile('output\s*=\s*brief').match(command.lower().strip()):
            full = False
        elif command.lower().strip() == "q":
            running = False
        else:
            tokenized_query = evaluate_query(command)
            print(tokenized_query)
        if full:
            show_extended_results(results(tokenized_query, full))
        else:
            show_brief_results(results(tokenized_query, full))
