from parser_module import Parse
from ranker import Ranker
import utils
import string


class Searcher:

    def __init__(self, inverted_index, config = None):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.ranker = Ranker()
        self.inverted_index = inverted_index

        self.config = config
        self.number_of_docs = 0
        self.upper_limit = 2000


    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {}
        posting_to_load = {}

        for term in query:
            if term not in self.inverted_index:
                continue
            utils.load_obj(self.inverted_index[term]["pointers"])

            if term[0] not in string.ascii_letters:
                if "SPECIALS" not in posting_to_load:
                    posting_to_load["SPECIALS"] = utils.load_obj(
                         f"{self.config.get_output_path()}\\postingSPECIALS")
            else:
                if term[0] not in posting_to_load:
                    posting_to_load[term[0]] = utils.load_obj(self.inverted_index[term]["pointers"])
                # elif term[0] != 'q' and term[0] != 'x' and term[0] != 'z':
            #     if term[0] not in posting_to_load:
            #         posting_to_load[term[0].lower()] = utils.load_obj(
            #             f"{self.config.get_output_path()}\\posting{term[0]}")
            # else:
            #     if "QXZ" not in posting_to_load:
            #         posting_to_load["q"] = utils.load_obj(
            #             f"{self.config.get_output_path()}\\postingQXZ")
            #         posting_to_load["x"] = utils.load_obj(
            #             f"{self.config.get_output_path()}\\postingQXZ")
            #         posting_to_load["z"] = utils.load_obj(
            #             f"{self.config.get_output_path()}\\postingQXZ")

        for term in query:
            try:  # an example of checks that you have to do
                if term[0] not in string.ascii_letters:
                    posting_doc = posting_to_load["SPECIALS"]
                else:
                    posting_doc = posting_to_load[term[0].lower()]

                for doc_tuple in posting_doc[term]["docs"]:
                    term_df = posting_doc[term]["df"]
                    term_tf = doc_tuple[1]
                    doc_id = doc_tuple[0]
                    max_tf = doc_tuple[2]
                    doc_len = doc_tuple[3]
                    curses_per_doc = doc_tuple[5]
                    if doc_id not in relevant_docs.keys():
                        # doc id: (number of words from query appeared in doc , [frequency of query words] , max_tf ,
                        #                            document length, number of docs that the appeared in,
                        #                                       number of curses in the doc
                        relevant_docs[doc_id] = [1, [term_tf], max_tf, doc_len, [term_df], curses_per_doc]
                        self.number_of_docs += 1
                        if self.number_of_docs > self.upper_limit:
                            break
                    else:
                        relevant_docs[doc_id][0] += 1
                        relevant_docs[doc_id][1].append(term_tf)
                        relevant_docs[doc_id][4].append(term_df)

            except Exception as e:
                print('term {} not found in posting'.format(term))

        return relevant_docs
