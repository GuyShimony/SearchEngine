from ranker import Ranker
import utils


# DO NOT MODIFY CLASS NAME
class Searcher:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit. The model 
    # parameter allows you to pass in a precomputed model that is already in 
    # memory for the searcher to use such as LSI, LDA, Word2vec models. 
    # MAKE SURE YOU DON'T LOAD A MODEL INTO MEMORY HERE AS THIS IS RUN AT QUERY TIME.
    def __init__(self, parser, indexer, model=None):
        self._parser = parser
        self._indexer = indexer
        self._ranker = Ranker()
        self._model = model
        self.number_of_docs = 0
        self.upper_limit = 2000
        self.inverted_index = self._indexer.inverted_idx


    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query, k=None):
        """ 
        Executes a query over an existing index and returns the number of 
        relevant docs and an ordered list of search results (tweet ids).
        Input:
            query - string.
            k - number of top results to return, default to everything.
        Output:
            A tuple containing the number of relevant search results, and 
            a list of tweet_ids where the first element is the most relavant 
            and the last is the least relevant result.
        """
        query_as_list = self._parser.parse_sentence(query)

        # relevant_docs = self._relevant_docs_from_posting(query_as_list)
        relevant_docs, query_weight = self._relevant_docs_from_posting(query_as_list)

        n_relevant = len(relevant_docs)
        ranked_doc_ids = Ranker.rank_relevant_docs(relevant_docs, query_weight)
        return n_relevant, ranked_doc_ids

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        # posting_list = self._indexer.get_term_posting_list(term)

    #     relevant_docs = {}
    #     for term in query_as_list:
    #         posting_list = self._indexer.get_term_posting_list(term)
    #         for doc_id, tf in posting_list:
    #             df = relevant_docs.get(doc_id, 0)
    #             relevant_docs[doc_id] = df + 1
    #     return relevant_docs
    #
        # TODO: Need to load the doc_index from indexer - get the posting list and for each doc load
        # it from the docs_index
        relevant_docs = {}
        Ranker.query_terms = {}
        query_as_list = sorted(query_as_list)  #  Sorting the query by words helps not opening the same posting twice
        for term in query_as_list:

            if term.lower() not in self.inverted_index and term.upper() not in self.inverted_index:
                continue
            elif term.lower() in self.inverted_index:
                term = term.lower()
            elif term.upper() in self.inverted_index:
                term = term.upper()

            # calculate each query term's weight
            if term in Ranker.query_terms:
                Ranker.query_terms[term] += 1
            else:
                Ranker.query_terms[term] = 1

        #  Check each first char in each term to see if the current open posting
        #  corresponds to char. For example if the current posting is the 'a' terms posting and the
        #  term we will look for is 'atom' we don't have to load the posting from disc again but to
        #  use the current loaded
        current_posting = ""
        posting_doc = 0
        query_weight = 0
        for term in Ranker.query_terms:
            try:

                if current_posting != self.inverted_index[term]["pointers"]:
                    posting_doc = utils.load_obj(self.inverted_index[term]["pointers"])
                    current_posting = self.inverted_index[term]["pointers"]

                query_weight += math.pow(Ranker.query_terms[term], 2)

                # TODO: Fix the doc_tuple
                for doc_tuple in posting_doc[term]["docs"]:
                    term_df = posting_doc[term]["df"]
                    doc_id = doc_tuple[0]
                    max_tf = self.docs_file[doc_id][1]
                    doc_len = self.docs_file[doc_id][2]
                    term_tf = round(0.6 * (doc_tuple[1] / max_tf) + 0.4 * (doc_tuple[1] / doc_len),3)
                    curses_per_doc = self.docs_file[doc_id][4]
                    term_tf_idf = term_tf * posting_doc[term]["idf"]  # normalized by max_tf and doc's length
                    doc_weight_squared = self.docs_file[doc_id][0]

                    if doc_id not in relevant_docs.keys():
                        # doc id: (number of words from query appeared in doc , [frequency of query words] , max_tf ,
                        #                            document length, ..
                        relevant_docs[doc_id] = [1, [term], max_tf, doc_len, curses_per_doc, [term_tf_idf], [term_tf],
                                                 [term_df], doc_weight_squared]
                        self.number_of_docs += 1
                        if self.number_of_docs > self.upper_limit:
                            break
                    else:
                        relevant_docs[doc_id][0] += 1
                        relevant_docs[doc_id][1].append(term)
                        relevant_docs[doc_id][5].append(term_tf_idf)
                        relevant_docs[doc_id][6].append(term_tf)
                        relevant_docs[doc_id][7].append(term_df)

            except Exception as e:
                pass
                #print('term {} not found in posting'.format(term))

        return relevant_docs, query_weight