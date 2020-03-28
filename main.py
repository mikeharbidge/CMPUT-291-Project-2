import re
from datetime import date

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
    pass


"""
Prints the summerized version of a result entry (the review id, the product title and the review score of all matching reviews)
@param result - a result entry returned by the query cursor
"""


def show_brief_results(result):
    pass


"""
Prints a result entry with additional details (all review fields)
@param result - a result entry returned by the query cursor
"""


def show_etxended_results(result):
    pass


if __name__ == "__main__":
    running = True
    while (running):
        full = False
        command = input("q to quit\n")
        if re.compile('output\s*=\s*full').match(command.lower().strip()):
            full = True
        elif re.compile('output\s*=\s*brief').match(command.lower().strip()):
            full = False
        elif command.lower().strip() == "q":
            running = False
        else:
            tokenized_query = evaluate_query(command)
            # print(tokenized_query)
            results(tokenized_query, full)
