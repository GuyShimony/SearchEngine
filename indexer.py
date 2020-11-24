import copy
import concurrent.futures
import utils
import os
from threading import Thread
import time
from posting_file_factory import PostingFilesFactory


class Indexer:

    def __init__(self, config):
        self.postings_factory = PostingFilesFactory.get_instance(config)
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.k = 200
        self.term_counter = 0
        self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
        self.posting_file_counter = 28  # postings a-z and (q,x,z) as 1  + specials + num + #, @
        self.posting_copy_for_saving = None
        if not os.path.exists(self.posting_dir_path):
            # Create a directory for all posting files
            os.makedirs(self.posting_dir_path)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

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

        max_tf = max(list(document_dictionary.values()))  # Get the most frequent used value
        terms_with_one_occurrence = 0
        number_of_curses = 0
        for term in document_dictionary:
            if document_dictionary[term] == 1:
                terms_with_one_occurrence += 1
            if term == "*CENSORED*":
                number_of_curses += 1

        # Go over each term in the doc
        for term in document_dictionary.keys():
            self.term_counter += 1
            # if self.term_counter > self.k:
            if len(self.postingDict) > self.k:
                self.term_counter = 0
                self.posting_copy_for_saving = copy.deepcopy(self.postingDict)
                # self.update_pointers()
                # self.posting_save()
                # self.executor.submit(self.update_pointers)
                self.executor.submit(self.posting_save)
                # Thread(target=self.update_posting_in_dict).start()
                # Thread(target=self.posting_save).start()  # Start each posting file saving process in a new thread
                self.postingDict.clear()

            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = {"freq": 1, "pointers": f"{self.postings_factory.get_file_path(term)}"}
                    # self.postingDict[term] = []
                else:
                    # freq -> number of occurrences in the whole corpus (for each term)
                    self.inverted_idx[term]["freq"] += 1

                if term not in self.postingDict.keys():
                    self.postingDict[term] = {"df": 1, "docs": [[document.tweet_id, document_dictionary[term],
                                                                 max_tf, document.doc_length,
                                                                 # TODO: Add a second param
                                                                 terms_with_one_occurrence, number_of_curses]]}
                else:
                    # tuples of tweet id , number of occurrences in the tweet
                    self.postingDict[term]["docs"].append([document.tweet_id, document_dictionary[term], max_tf,
                                                           document.doc_length, terms_with_one_occurrence,
                                                           number_of_curses])
                    # number of tweets the term appeared in
                    self.postingDict[term]['df'] += 1

            except Exception as e:
                print('problem with the following key {}'.format(term))
                print(str(e))

    def posting_save(self):
        terms_for_saving = {}
        for term in self.posting_copy_for_saving:
            if term[0] in terms_for_saving:
                terms_for_saving[term[0]].append(term)
            else:
                terms_for_saving[term[0]] = [term]

        posting_file = ''
        posting_file_name = ''
        for letter in terms_for_saving:
            posting_file, posting_file_name = self.postings_factory.get_posting_file_and_path(letter)

            for term in terms_for_saving[letter]:
                # update term's pointer
                # self.inverted_idx[term]['pointers'] = posting_file_name
                if term in posting_file:
                    posting_file[term]["docs"] = posting_file[term]["docs"] + self.posting_copy_for_saving[term]["docs"]
                else:
                    posting_file[term] = self.posting_copy_for_saving[term]
            try:
                utils.save_obj(posting_file, posting_file_name)

            except Exception as e:
                print(str(e))

    def __del__(self):
        if len(self.postingDict) > 0 and len(self.postingDict) < self.k:
            self.posting_save()
            self.postingDict.clear()
        # sorted(self.inverted_idx.items(), key=lambda item: item[0], reverse=True)
        # self.executor.shutdown()

    def get_pool_executer(self):
        return self.executor
