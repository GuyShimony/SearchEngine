from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer, os
from searcher import Searcher
import utils
from time import time
import re


def run_engine(corpus_path=None, output_path=None, stemming=False, queries=None, num_docs_to_retrieve=None):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass()
    config.corpusPath = corpus_path
    config.set_output_path(output_path)
    config.toStem = stemming

    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)

    documents_list = r.read_file(file_name='sample2.parquet')
    # documents_list = r.read_file(file_name='Data')
    # Iterate over every document in the file
    start = time()
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)
    print('Finished parsing and indexing. Starting to export files')
    print(time() - start)
    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    if corpus_path is None or output_path is None or stemming is None \
            or queries is None or num_docs_to_retrieve is None:
        raise ValueError("Arguments can't be None")

    if corpus_path == '' or output_path == '':
        raise ValueError("A valid path should be given")

    run_engine(corpus_path, output_path, stemming)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    if os.path.isfile(queries):  # If the queries are stored in a file #TODO: check
        with open(queries, encoding="utf8") as file:
            queries = file.readlines()

    k = num_docs_to_retrieve
    inverted_index = load_index()
    for query in queries:
        if query != '\n':
            if re.search(r'\d', query):  # remove number query and "." from query if exists
                query = query.replace(re.findall("\d.[\s]*", query)[0], "")
            for doc_tuple in search_and_rank_query(query, inverted_index, k):
                print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
