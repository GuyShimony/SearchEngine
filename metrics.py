import traceback
import math
import pandas as pd
from functools import reduce

df = pd.DataFrame(
    {'query': [1, 1, 2, 2, 3], 'Tweet_id': [12345, 12346, 12347, 12348, 12349],
     'label': [1, 0, 1, 1, 0]})

# df = pd.read_csv("311178123.csv")

test_number = 0
results = []


# precision(df, True, 1) == 0.5
# precision(df, False, None) == 0.5
def precision(df, single=False, query_number=None):
    """
        This function will calculate the precision of a given query or of the entire DataFrame
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param single: Boolean: True/False that tell if the function will run on a single query or the entire df
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :return: Double - The precision
    """
    if single:
        df = df.loc[df['query'] == query_number]
        total_relevance = df['label'].aggregate('sum')
        total = df.shape[0]

    else:
        queries = df['query'].tolist()
        queries = set(queries)
        total_precision = 0
        for query in queries:
            query_df = df.loc[df['query'] == query]
            query_relevance = query_df['label'].aggregate('sum')
            total = query_df.shape[0]
            total_precision += round(query_relevance / total, 2)

        total_relevance = total_precision
        total = len(queries)

    return total_relevance / total


# recall(df, {1:2}, True) == 0.5
# recall(df, {1:2, 2:3, 3:1}, False) == 0.388
def recall(df, num_of_relevant):
    """
        This function will calculate the recall of a specific query or of the entire DataFrame
        :param df: DataFrame: Contains query numbers, tweet ids, and label
        :param num_of_relevant: Dictionary: number of relevant tweets for each query number. keys are the query number
         and values are the number of relevant.
        :return: Double - The recall
    """

    queries = num_of_relevant.keys()
    total_recall = 0
    total = len(num_of_relevant)
    for query in queries:
        query_df = df.loc[df['query'] == query]
        total_relevance = query_df['label'].aggregate('sum')
        if num_of_relevant[query]:
            total_recall += total_relevance / num_of_relevant[query]
        else:
            total -= 1
    return total_recall / len(num_of_relevant)


# precision_at_n(df, 1, 2) == 0.5
# precision_at_n(df, 3, 1) == 0
def precision_at_n(df, query_number=1, n=5):
    """
        This function will calculate the precision of the first n files in a given query.
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :param n: Total document to splice from the df
        :return: Double: The precision of those n documents
    """
    if query_number:
        df = df.loc[df['query'] == query_number]
        df = df.head(n)
        total_relevance = df['label'].aggregate('sum')
        total = df.shape[0]
    else:
        # df = df.head(n)
        # total_relevance = df['label'].aggregate('sum')
        # total = df.shape[0]
        queries = df['query'].tolist()
        queries = set(queries)
        total_precision = 0
        for query in queries:
            query_df = df.loc[df['query'] == query]
            query_n_df = query_df.head(n)
            query_relevance = query_n_df['label'].aggregate('sum')
            total = query_n_df.shape[0]
            total_precision += round(query_relevance / total, 2)

        total_relevance = total_precision
        total = len(queries)

    return total_relevance / total


# map(df) == 0.5
def map(df):
    """
        This function will calculate the mean precision of all the df.
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :return: Double: the average precision of the df
    """
    map = []
    queries = df['query'].tolist()
    queries = set(queries)
    total_precision = 0
    for query in queries:
        precision_at_recall_for_query = []
        query_df = df.loc[df['query'] == query]
        n = 0
        for i in query_df['label'].tolist():
            n += 1
            if i == 1:
                # query_n_df = query_df.head(n)
                # query_relevance = query_n_df['label'].aggregate('sum')
                # total = query_n_df.shape[0]
                # total_precision += round(query_relevance / total, 2)
                precision_at_recall_for_query.append(precision_at_n(df, query, n))
        if precision_at_recall_for_query:  # for queries with only non relevant docs
            precision_avg = sum(precision_at_recall_for_query) / len(precision_at_recall_for_query)
            map.append(precision_avg)
        else:
            map.append(0)

    return sum(map) / len(map)


def run_value(func, expected, variables):
    """
        This function is used to test your code. Do Not change it!!
        :param func: Function: The function to test
        :param expected: Float: The expected value from the function
        :param variables: List: a list of variables for the function
    """
    global test_number, results
    test_number += 1
    result = func(*variables)
    try:
        result = float(f'{result:.3f}')
        if abs(result - float(f'{expected:.3f}')) <= 0.01:
            results.extend([f'Test: {test_number} passed'])
        else:
            results.extend([f'Test: {test_number} Failed running: {func.__name__}'
                            f' expected: {expected} but got {result}'])
    except ValueError as ve:
        results.extend([f'Test: {test_number} Failed running: {func.__name__}'
                        f' value return is not a number'])
    except:
        d = traceback.format_exc().splitlines()
        results.extend([f'Test: {test_number} Failed running: {func.__name__} with the following error: {" ".join(d)}'])


run_value(precision, 0.5, [df, True, 1])
run_value(precision, 0.5, [df, False, None])
run_value(recall, 0.5, [df, {1: 2}])
run_value(recall, 0.388, [df, {1: 2, 2: 3, 3: 1}])
run_value(precision_at_n, 0.5, [df, 1, 2])
run_value(precision_at_n, 0, [df, 3, 1])
run_value(map, 2 / 3, [df])



for res in results:
    print(res)
