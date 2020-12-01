import math
from ranker import Ranker
import utils
from posting_file_factory import PostingFilesFactory


class Searcher:

    def __init__(self, inverted_index, config=None, docs_data=None):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.ranker = Ranker()
        self.postings_factory = PostingFilesFactory.get_instance(config)
        self.inverted_index = inverted_index
        self.docs_data = docs_data
        self.config = config
        self.number_of_docs = 0
        self.upper_limit = 2000
        self.docs_file = self.postings_factory.get_docs_file()

    #    self.words_tf_idf = Indexer.word_tf_idf

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        postings_factory = PostingFilesFactory.get_instance(self.config)
        relevant_docs = {}
        Ranker.query_terms = {}
        query = sorted(query)  #  Sorting the query by words helps not opening the same posting twice
        for term in query:

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
            # posting_file_name = postings_factory.get_file_path(term)

            # if "SPECIALS" in posting_file_name:
            #     if "SPECIALS" not in posting_to_load:
            #         posting_to_load["SPECIALS"] = utils.load_obj(self.inverted_index[term]["pointers"])
            # else:
            #     if self.inverted_index[term]["pointers"] not in postings_loaded:
            #
            #         posting_to_load[term[0].lower()] = utils.load_obj(self.inverted_index[term]["pointers"])
            #         postings_loaded[self.inverted_index[term]["pointers"]] = posting_to_load[term[0].lower()]
            #     else:
            #         posting_to_load[term[0].lower()] = postings_loaded[self.inverted_index[term]["pointers"]]

        current_posting = ""
        posting_doc = 0
        query_weight = 0
        for term in Ranker.query_terms:
            try:

                #posting_file_name = postings_factory.get_file_path(term)
                if current_posting != self.inverted_index[term]["pointers"]:
                    posting_doc = utils.load_obj(self.inverted_index[term]["pointers"])
                    current_posting = self.inverted_index[term]["pointers"]
                    # Check if the new term needs a different posting file
                    # if "SPECIALS" in posting_file_name:
                    #     # posting_doc = posting_to_load["SPECIALS"]
                    #     posting_doc = self.inverted_index[term]["pointers"]
                    # else:
                    #     # posting_doc = posting_to_load[term[0].lower()]
                    #     posting_doc = posting_to_load[term[0].lower()]

                query_weight += math.pow(Ranker.query_terms[term], 2)

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
                print('term {} not found in posting'.format(term))

        return relevant_docs, query_weight
