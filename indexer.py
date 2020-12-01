import utils
import os
from posting_file_factory import PostingFilesFactory
import string


class Indexer:

    def __init__(self, config):

        self.docs_data = {}
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.max_documents = 80000
        self.docs_counter = 0
        self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
        if not os.path.exists(self.posting_dir_path):
            # Create a directory for all posting files
            os.makedirs(self.posting_dir_path)
            os.makedirs(self.posting_dir_path + "\\docs")
        self.postings_factory = PostingFilesFactory.get_instance(config)
        self.lower_case_words = {}

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        self.docs_counter += 1
        document_dictionary = document.term_doc_dictionary
        if not document_dictionary:
            return
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

        if self.docs_counter > self.max_documents:
            self.docs_counter = 0
            self.posting_save()
            self.postingDict.clear()

        self.docs_data[document.tweet_id] = [0, max_tf, document.doc_length, terms_with_one_occurrence,
                                             number_of_curses]

        for term in document_dictionary.keys():
            try:
                if not doc_max_term and document_dictionary[term] == max_tf:
                    doc_max_term = term
                    self.docs_data[document.tweet_id].append(doc_max_term)
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    # check if term was already added as upper (and should now be lower)
                    if term.islower() and term.upper() in self.inverted_idx:
                        # remove upper term and update it as a lower term
                        self.inverted_idx[term] = self.inverted_idx[term.upper()]
                        self.inverted_idx[term]["freq"] += 1
                        self.inverted_idx.pop(term.upper())

                    else:  # term is not in the dictionary in any form (case)
                        self.inverted_idx[term] = {"freq": 1,
                                                   "pointers": f"{self.postings_factory.get_file_path(term.lower())}"}

                else:
                    # freq -> number of occurrences in the whole corpus (for each term df)
                    self.inverted_idx[term]["freq"] += 1

                if term not in self.postingDict:
                    # check if term was already added as upper (and should now be lower)
                    if term.islower() and term.upper() in self.postingDict:
                        # update the term's data
                        self.postingDict[term] = self.postingDict[term.upper()]

                        # update the term --> appeared again
                        self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term],
                                                               document.doc_length])
                        # number of tweets the term appeared in
                        self.postingDict[term]['df'] += 1
                        # remove the upper case term
                        self.postingDict.pop(term.upper())

                    else:  # new term
                        self.postingDict[term] = {"df": 1, "docs": [[document.tweet_id, document_dictionary[term],
                                                                     document.doc_length]]}

                else:
                    self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term],
                                                           document.doc_length])
                    # number of tweets the term appeared in
                    self.postingDict[term]['df'] += 1

            except Exception as e:
                print('problem with the following key {}'.format(term))

    def posting_save(self):
        """
        The function will sort all the words in a dictionary based on the first char.
        For example: {'a': ['atom','array'], 'b'['bar']}.
        It will used the mapping to save each term of the terms that were filed up until the function call
        and save each term to the posting file.
        """
        terms_for_saving = {}
        for term in self.postingDict:
            lower_term = term.lower()
            if lower_term[0] in terms_for_saving:
                terms_for_saving[lower_term[0]].append(term)
            else:
                terms_for_saving[lower_term[0]] = [term]

        self.postings_factory.create_posting_files(self.postingDict, terms_for_saving)

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

    def cleanup(self, corpus_size):
        """
        The function will be responsible to free up the memory of all objects
        created by the indexer and save up the last docs that were not saved in any terms
        """
        # insert each word's tf-idf value for each document --> {doc.id: [term tf, term tf_idf for doc]}
        if len(self.postingDict) > 0:
            self.posting_save()

        utils.save_obj(self.inverted_idx, "inverted_idx")
        self.inverted_idx.clear()

        utils.save_obj(self.docs_data, f"{self.posting_dir_path}\\docs\\docs_index")
        self.docs_data.clear()
        self.postings_factory.merge(corpus_size)


