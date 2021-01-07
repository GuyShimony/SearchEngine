from ranker import Ranker
import utils
import math
from SpellChecker import SpellCheck


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
        self.inverted_index = self._indexer.get_inverted_index()
        self.docs_index = self._indexer.get_docs_index()
        Ranker.avdl = self._indexer.total_docs_len / self._indexer.get_docs_count()

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
        query_as_list = self._model.spellCheck(query_as_list) #TODO:ASF
        #query_as_list = SpellCheck.spellCheck(query_as_list)

        relevant_docs, Ranker.query_weight = self._relevant_docs_from_posting(query_as_list)
        ranked_doc_ids = Ranker.rank_relevant_docs(relevant_docs, self._indexer.get_docs_count())
        n_relevant = len(ranked_doc_ids)
        ranked_doc_ids = [doc_id for doc_id, rank in ranked_doc_ids]

        return n_relevant, ranked_doc_ids
        # return n_relevant, ranked_doc_ids, relevant_docs

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """

        relevant_docs = {}
        Ranker.query_terms = {}
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

        query_weight = 0
        for term in Ranker.query_terms:
            try:

                query_weight += math.pow(Ranker.query_terms[term], 2)
                try:
                    for doc_id in self._indexer.get_term_posting_list(term):
                        normalized_tf = self.inverted_index[term]["posting_list"][doc_id][0]
                        term_df = self.inverted_index[term]["df"]
                        # term_tf = round(0.6 * (tf / max_tf) + 0.4 * (tf / doc_len),3) # Maybe try again max_tf with doc len
                        doc_len = self.docs_index[doc_id][2]
                        # term_tf = round((tf / doc_len), 3) # Maybe try again max_tf with doc len
                        term_tf_idf = self.inverted_index[term]["posting_list"][doc_id][
                            1]  # normalized by max_tf and doc's length

                        if doc_id not in relevant_docs.keys():
                            curses_per_doc = self.docs_index[doc_id][4]
                            # already calculated per doc, need to get and insert only once
                            max_tf = self.docs_index[doc_id][1]
                            doc_weight_squared = self.docs_index[doc_id][0]

                            # doc id: (number of words from query appeared in doc , [frequency of query words] , max_tf ,
                            #                            document length, ..
                            relevant_docs[doc_id] = [1, [term], max_tf, doc_len, curses_per_doc, [term_tf_idf],
                                                     [normalized_tf],
                                                     [term_df],
                                                     doc_weight_squared]  # curses_per_doc was deleted from index 4

                        else:
                            relevant_docs[doc_id][0] += 1
                            relevant_docs[doc_id][1].append(term)
                            relevant_docs[doc_id][5].append(term_tf_idf)
                            relevant_docs[doc_id][6].append(normalized_tf)
                            relevant_docs[doc_id][7].append(term_df)
                except Exception as e:
                    print("DSF")
            except Exception as e:
                # pass
                print('term {} not found in inverted index'.format(term))

        return relevant_docs, query_weight
