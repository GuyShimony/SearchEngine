import utils
import string
import numpy as np


# DO NOT MODIFY CLASS NAME
class Indexer:

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def __init__(self, config):
        self.docs_index = {}
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.lower_case_words = {}
        self.docs_counter = 0
        self.total_docs_len = 0

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        if not document_dictionary:
            return
        self.docs_counter += 1

        document_dictionary = self.capital_letters(document_dictionary)  # get dictionary according to lower and upper
        doc_max_term = ''
        max_tf = max(list(document_dictionary.values()))  # Get the most frequent used value
        terms_with_one_occurrence = 0
        number_of_curses = 0

        for term in document_dictionary:
            if document_dictionary[term] == 1:
                terms_with_one_occurrence += 1
            if term == "*CENSORED*":
                number_of_curses += 1

        self.docs_index[document.tweet_id] = [0, max_tf, document.doc_length, terms_with_one_occurrence,
                                              number_of_curses, np.zeros(50)]

        self.total_docs_len += document.doc_length

        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                if not doc_max_term and document_dictionary[term] == max_tf:
                    doc_max_term = term
                    self.docs_index[document.tweet_id].append(doc_max_term)
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    # check if term was already added as upper (and should now be lower)
                    if term.islower() and term.upper() in self.inverted_idx:
                        # remove upper term and update it as a lower term
                        self.inverted_idx[term] = self.inverted_idx[term.upper()]
                        self.inverted_idx[term]["freq"] += 1
                        self.inverted_idx[term]["df"] += 1
                        self.inverted_idx[term]["posting_list"][document.tweet_id] = [
                            document_dictionary[term] / document.doc_length]
                        self.inverted_idx.pop(term.upper())

                    else:  # term is not in the dictionary in any form (case)
                        self.inverted_idx[term] = {"freq": 1,
                                                   "df": 1,
                                                   "posting_list": {document.tweet_id: [
                                                       document_dictionary[term] / document.doc_length]}}

                else:
                    # term is in the inverted_index
                    self.inverted_idx[term]["freq"] += 1
                    self.inverted_idx[term]["df"] += 1
                    self.inverted_idx[term]["posting_list"][document.tweet_id] = [
                        document_dictionary[term] / document.doc_length]

                # self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

            except Exception as e:
                print(str(e))
                print('problem with the following key {}'.format(term[0]))

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        return utils.load_objects(fn)[0]

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def save_index(self, fn):
        """
        Saves a pre-computed index (or indices) so we can save our work.
        Input:
              fn - file name of pickled index.
        """
        index_with_docs_index = [self.inverted_idx, self.docs_index]
        utils.save_objects(index_with_docs_index, fn)

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def _is_term_exist(self, term):
        """
        Checks if a term exist in the dictionary.
        """
        return term in self.inverted_idx

    # feel free to change the signature and/or implementation of this function
    # or drop altogether.
    def get_term_posting_list(self, term):
        """
        Return the posting list from the index for a term.
        """
        return self.inverted_idx[term]["posting_list"] if self._is_term_exist(term) else []

    def capital_letters(self, document_dictionary):
        """
        :param document_dictionary: dictionary holding the tokenized term and the amount of its appearances
        :return: document_dictionary_new: will hold the same terms as the original only with caps or lower letters
        accordingly
        """
        document_dictionary_new = {}  # dictionary with words saved correctly by capitals
        for term in document_dictionary:
            if not term:
                continue
            if term[0] not in string.ascii_letters:
                document_dictionary_new[term] = document_dictionary[term]
                continue
            if term[0].islower():

                document_dictionary_new[term] = document_dictionary[term]
            else:  # term is upper case
                lower_term = term.lower()
                if lower_term in self.inverted_idx:
                    document_dictionary_new[lower_term] = document_dictionary[term]
                else:  # giving it a chance as upper case
                    document_dictionary_new[term.upper()] = document_dictionary[term]
        if not document_dictionary_new:
            return document_dictionary
        return document_dictionary_new

    def get_inverted_index(self):
        return self.inverted_idx

    def get_docs_index(self):
        return self.docs_index

    def get_docs_count(self):
        return self.docs_counter

    def __getitem__(self, term_and_item):
        if term_and_item[0] in self.inverted_idx:
            return self.inverted_idx[term_and_item[0]][term_and_item[1]]

        elif [term_and_item[0]] in self.docs_index:
            return self.docs_index[[term_and_item[0]]][term_and_item[1]]

        else:
            #  The item is not in either the idx and docs_index
            return None
