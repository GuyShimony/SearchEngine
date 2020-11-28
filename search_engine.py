from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer, os
from searcher import Searcher
import utils
from time import time
import re
import pandas as pd
import shutil

conifg = None
number_of_documents = 0


def run_engine(corpus_path=None, output_path=None, stemming=False, queries=None, num_docs_to_retrieve=None):
    """

    :return:
    """
    global config, number_of_documents

    number_of_documents = 0

    config = ConfigClass()
    config.corpusPath = corpus_path
    config.set_output_path(output_path)
    config.toStem = stemming
    if os.path.exists(config.get_output_path()): #TODO: check if to delete
        shutil.rmtree(config.get_output_path())

    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(config.toStem)
    indexer = Indexer(config)

    # executer = indexer.get_pool_executer()

    documents_list = r.read_file(file_name='samples')
    # documents_list = r.read_file(file_name='Data')
    # Iterate over every document in the file
    start = time()
    print(start)
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)
    indexer.cleanup()
    print('Finished parsing and indexing. Starting to export files')
    print(time() - start)
    utils.save_obj(indexer.inverted_idx, "inverted_idx")


# utils.save_obj(indexer.docs_data, "docs_weights")
# utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def load_docs_data():
    global config
    print('Load docs dictionary')
    docs_data = utils.load_obj(f"{config.get_output_path()}\\docs\\postingdocs_index")
    return docs_data


def search_and_rank_query(query, inverted_index, k, docs_data=None):
    global config
    p = Parse(config.toStem)
    query_as_list = p.parse_sentence(query)
    # docs_data = load_docs_data()
    searcher = Searcher(inverted_index, config, docs_data)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, number_of_documents)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    if corpus_path is None or output_path is None or stemming is None \
            or queries is None or num_docs_to_retrieve is None:
        raise ValueError("Arguments can't be None")

    if not corpus_path:
        raise ValueError("Must pass corpus_path")

    if not output_path:
        output_path = os.getcwd()

    run_engine(corpus_path, output_path, stemming)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    if os.path.isfile(queries):  # If the queries are stored in a file
        with open(queries, encoding="utf8") as file:
            queries = file.readlines()

    result = pd.DataFrame(columns=['Query_num', 'Tweet_id', 'Rank'])
    k = num_docs_to_retrieve
    inverted_index = load_index()
    docs_data = load_docs_data()

    for query in queries:
        if query != '\n':
            if re.search(r'\d', query):  # remove number query and "." from query if exists
                query_num = re.findall("\d+", query)[0]
                query = query.replace(re.findall("\d.[\s]*", query)[0], "")
            else:
                query_num = 0
            # print("Starting to search query: {0}".format(query))
            for doc_tuple in search_and_rank_query(query, inverted_index, k, docs_data):
                result = result.append({"Query_num": query_num, "Tweet_id": doc_tuple[0], "Rank": doc_tuple[1]},
                                       ignore_index=True)

    result.to_csv(os.path.join(output_path, "results.csv"))
