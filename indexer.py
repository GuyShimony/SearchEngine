import concurrent.futures
import time

import utils
import os
from threading import Thread
from posting_file_factory import PostingFilesFactory
import string
import math


class Indexer:
    word_tf_idf = {}

    def __init__(self, config):

        self.docs_data = {}

        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.max_documents = 10000
        self.docs_counter = 0
        self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
        self.posting_copy_for_saving = None
        if not os.path.exists(self.posting_dir_path):
            # Create a directory for all posting files
            os.makedirs(self.posting_dir_path)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.postings_factory = PostingFilesFactory.get_instance(config)
        self.postings_factory.set_threadpool(self.executor)
        self.lower_case_words = {}
        self.number_of_docs = 0
        self.letters_appeared = []

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
        self.number_of_docs += 1
        document_dictionary = self.capital_letters(document_dictionary)  # get dictionary according to lower and upper

        max_tf = max(list(document_dictionary.values()))  # Get the most frequent used value
        terms_with_one_occurrence = 0
        number_of_curses = 0
        for term in document_dictionary:
            if document_dictionary[term] == 1:
                terms_with_one_occurrence += 1
            if term == "*CENSORED*":
                number_of_curses += 1
        # Indexer.docs_weights[document.tweet_id] = [0, max_tf]
        self.docs_data[document.tweet_id] = [0, max_tf, document.doc_length, terms_with_one_occurrence,
                                             number_of_curses]

        # Go over each term in the doc
        for term in document_dictionary.keys():
            # self.docs_counter += 1
            # if self.term_counter > self.k:
            if len(self.postingDict) > self.max_documents:
                self.docs_counter = 0
                self.posting_copy_for_saving = self.postingDict.copy()
                #self.executor.submit(self.posting_save)
                self.posting_save()
                self.postingDict.clear()

            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    # check if term was already added as upper (and should now be lower)
                    if term.islower() and term.upper() in self.inverted_idx:
                        # remove upper term and update it as a lower term
                        self.inverted_idx[term] = self.inverted_idx[term.upper()]
                        Indexer.word_tf_idf[term] = Indexer.word_tf_idf[term.upper()]

                        self.inverted_idx[term]["freq"] += 1
                        Indexer.word_tf_idf[term][document.tweet_id] = [
                            document_dictionary[term] / max_tf]  # tf normalized

                        self.inverted_idx.pop(term.upper())
                        Indexer.word_tf_idf.pop(term.upper())
                    else:  # term is not in the dictionary in any form (case)
                        self.inverted_idx[term] = {"freq": 1,
                                                   "pointers": f"{self.postings_factory.get_file_path(term.lower())}"}
                        Indexer.word_tf_idf[term] = {document.tweet_id: [document_dictionary[term] / max_tf]}

                    # self.postingDict[term] = []
                else:
                    # freq -> number of occurrences in the whole corpus (for each term df)
                    self.inverted_idx[term]["freq"] += 1
                    Indexer.word_tf_idf[term][document.tweet_id] = [document_dictionary[term] / max_tf]

                if term not in self.postingDict:
                    # check if term was already added as upper (and should now be lower)
                    if term.islower() and term.upper() in self.postingDict:
                        # update the term's data
                        self.postingDict[term] = self.postingDict[term.upper()]
                        #    Indexer.word_tf_idf[term] = Indexer.word_tf_idf[term.upper()]

                        # update the term --> appeared again
                        # self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term], max_tf,
                        #                                        document.doc_length, terms_with_one_occurrence,
                        #                                        number_of_curses])

                        self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term],
                                                               document.doc_length])
                        # number of tweets the term appeared in
                        self.postingDict[term]['df'] += 1

                        # remove the upper case term
                        self.postingDict.pop(term.upper())
                    #     Indexer.word_tf_idf.pop(term.upper())

                    # update the term's tf and df
                    # self.postingDict[term]['tf-idf'][1] += 1
                    #   Indexer.word_tf_idf[term][document.tweet_id].append([document_dictionary[term]])
                    #  Indexer.word_tf_idf[term].append([document.tweet_id, document_dictionary[term]])

                    else:  # new term
                        # self.postingDict[term] = {"df": 1, "docs": [[document.tweet_id, document_dictionary[term],
                        #                                              max_tf, document.doc_length,
                        #                                              # TODO: Add a second param
                        #                                              terms_with_one_occurrence, number_of_curses]]}
                        self.postingDict[term] = {"df": 1, "docs": [[document.tweet_id, document_dictionary[term],
                                                                     document.doc_length]]}

                        # Indexer.word_tf_idf[term] = [[document.tweet_id, document_dictionary[term]]]
                    #  Indexer.word_tf_idf[term] = {document.tweet_id: [document_dictionary[term]]}
                else:
                    # tweet id , number of occurrences in the tweet (tf) ....
                    # self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term], max_tf,
                    #                                        document.doc_length, terms_with_one_occurrence,
                    #                                        number_of_curses])
                    self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term],
                                                           document.doc_length])
                    # number of tweets the term appeared in
                    self.postingDict[term]['df'] += 1

                    # update the term's tf and df
                    # self.postingDict[term]['tf-idf'][1] += 1
                    # .append([document.tweet_id, document_dictionary[term]])

            except Exception as e:
                print('problem with the following key {}'.format(term))

    def posting_save(self):
        terms_for_saving = {}
        for term in self.posting_copy_for_saving:
            lower_term = term.lower()
            if lower_term[0] in terms_for_saving:
                terms_for_saving[lower_term[0]].append(term)
            else:
                self.letters_appeared.append(lower_term[0])
                terms_for_saving[lower_term[0]] = [term]

        self.postings_factory.create_posting_files(self.posting_copy_for_saving, terms_for_saving)
        #self.executor.submit(self.postings_factory.create_posting_files, self.posting_copy_for_saving, terms_for_saving)
        # posting_file = ''
        # posting_file_name = ''
        # for letter in terms_for_saving:
        #     lower_letter = letter.lower()
        #     posting_file, posting_file_name = self.postings_factory.get_posting_file_and_path(lower_letter)
        #
        #     for term in terms_for_saving[lower_letter]:
        #         # update term's pointer
        #         # self.inverted_idx[term]['pointers'] = posting_file_name
        #         if term in posting_file:
        #             posting_file[term]["docs"] = posting_file[term]["docs"] + self.posting_copy_for_saving[term]["docs"]
        #             posting_file[term]["df"] += self.posting_copy_for_saving[term]["df"]
        #         else:
        #             posting_file[term] = self.posting_copy_for_saving[term]
        #     try:
        #         utils.save_obj(posting_file, posting_file_name)
        #
        #     except Exception as e:
        #         print(str(e))

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
            if term[0] not in string.ascii_letters:  #  TODO: Check where the "" comes from
                document_dictionary_new[term] = document_dictionary[term]
                continue
            if term[0].islower():
                if term not in self.lower_case_words:
                    self.lower_case_words[term] = 0
                document_dictionary_new[term] = document_dictionary[term]
            else:  # term is upper case
                lower_term = term.lower()
                if lower_term in self.lower_case_words:  # term was seen in small letters
                    document_dictionary_new[lower_term] = document_dictionary[term]
                else:  # giving it a chance as upper case
                    document_dictionary_new[term.upper()] = document_dictionary[term]
        if not document_dictionary_new:
            return document_dictionary
        return document_dictionary_new

    #def __del__(self):
    def cleanup(self):
        # insert each word's tf-idf value for each document --> {doc.id: [term tf, term tf_idf for doc]}
        if len(self.postingDict) > 0:
            self.posting_copy_for_saving = self.postingDict
            self.posting_save()
        # for letter in self.letters_appeared:
        #     self.postings_factory.merge_file_group(letter)
        # for thread in self.postings_factory.merge():
            self.postings_factory.merge()
        #     thread.join()
        print(time.time())
        for term in self.word_tf_idf:  #  TODO: Need to think of speed up - taking too long
            posting_file, posting_file_name = self.postings_factory.get_posting_file_and_path(term)
            # calculate idf for each word
            term_df = posting_file[term]["df"]
            term_idf = math.log10(self.number_of_docs / term_df)
            for term_doc in self.word_tf_idf[term]:
                self.word_tf_idf[term][term_doc].append(term_idf * self.word_tf_idf[term][term_doc][0])
                self.docs_data[term_doc][0] += math.pow(self.word_tf_idf[term][term_doc][1], 2)
        # sorted(self.inverted_idx.items(), key=lambda item: item[0], reverse=True)
        # self.executor.shutdown()

    def get_pool_executer(self):
        return self.executor
