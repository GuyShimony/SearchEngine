import pandas as pd
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import math


# DO NOT CHANGE THE CLASS NAME
class SearchEngine:

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation, but you must have a parser and an indexer.
    def __init__(self, config=None):
        self._config = config

        if self._config:
            if not hasattr(self._config, 'toStem'):
                self._config.toStem = False
            if not hasattr(self._config, 'toLemm'):
                self._config.toLemm = False

        self._parser = Parse()
        self._indexer = Indexer(config)
        self._model = None
        self.corpus_size = 0

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def build_index_from_parquet(self, fn):
        """
        Reads parquet file and passes it to the parser, then indexer.
        Input:
            fn - path to parquet file
        Output:
            No output, just modifies the internal _indexer object.
        """
        df = pd.read_parquet(fn, engine="pyarrow")
        documents_list = df.values.tolist()
        # Iterate over every document in the file
        number_of_documents = 0
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = self._parser.parse_doc(document)
            number_of_documents += 1
            # index the document data
            self._indexer.add_new_doc(parsed_document)
        self._indexer.save_index(self._config.get_output_path())  # Save the inverted_index to disk
        self.corpus_size = self._indexer.get_docs_count()
        self.calculate_doc_weight()

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        self._indexer.load_index(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_precomputed_model(self, model_dir=None):
        """
        Loads a pre-computed model (or models) so we can answer queries.
        This is where you would load models like word2vec, LSI, LDA, etc. and
        assign to self._model, which is passed on to the searcher at query time.
        """
        pass

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query):
        """
        Executes a query over an existing index and returns the number of
        relevant docs and an ordered list of search results.
        Input:
            query - string.
        Output:
            A tuple containing the number of relevant search results, and
            a list of tweet_ids where the first element is the most relavant
            and the last is the least relevant result.
        """
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        return searcher.search(query)

    def calculate_doc_weight(self):
        """
       The method calculates the TF-IDF for each document
       :return:
       """
        inverted_index = self._indexer.inverted_idx
        docs_index = self._indexer.get_docs_index()

        for word in inverted_index:

            for doc_id in self._indexer.get_term_posting_list(word):
                normalized_term_tf = inverted_index[word]["posting_list"][doc_id][0]
                term_df = inverted_index[word]['df']
                term_idf = math.log2(self.corpus_size / term_df)
                # calculate doc's total weight
                term_weight = normalized_term_tf * term_idf
                inverted_index[word]["posting_list"][doc_id].append(term_weight)
                term_weight_squared = math.pow(term_weight, 2)
                docs_index[doc_id][0] += term_weight_squared
                docs_index[doc_id][0] = round(docs_index[doc_id][0], 3)

