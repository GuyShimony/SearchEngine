import copy
import concurrent.futures
import utils
import os
import string


class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.k = 10000
        self.term_counter = 0
        self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
        self.posting_file_counter = 25  # postings a-z and (q,x,z) as 1  + specials file
        self.posting_copy_for_saving = None
        if not os.path.exists(self.posting_dir_path):
            # Create a directory for all posting files
            os.makedirs(self.posting_dir_path)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        # TODO: Do it in a thread?
        self.create_postings_AtoZ()  # create all postings -- > init with None

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary
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
            if self.term_counter > self.k:
                self.term_counter = 0
                self.posting_copy_for_saving = copy.deepcopy(self.postingDict)
                self.executor.submit(self.update_pointers)
                self.executor.submit(self.posting_save)
                # Thread(target=self.update_posting_in_dict).start()
                # Thread(target=self.posting_save).start()  # Start each posting file saving process in a new thread
                self.postingDict.clear()

            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = {"freq": 1, "pointers": []}
                    # self.postingDict[term] = []
                else:
                    # freq -> number of occurrences in the whole corpus (for each term)
                    self.inverted_idx[term]["freq"] += 1

                if term not in self.postingDict.keys():
                    self.postingDict[term] = {"df": 1, "tweets": [(document.tweet_id, document_dictionary[term],
                                                                   max_tf, # TODO: Add a second param
                                                                   terms_with_one_occurrence, number_of_curses)]}
                else:
                    # tuples of tweet id , number of occurrences in the tweet
                    self.postingDict[term]["tweets"].append((document.tweet_id, document_dictionary[term], max_tf,
                                                             terms_with_one_occurrence, number_of_curses))
                    # number of tweets the term appeared in
                    self.postingDict[term]['df'] += 1

            except Exception as e:
                print('problem with the following key {}'.format(term))
                print(str(e))

    def posting_save(self):
        pass

    #     utils.save_obj(self.posting_copy_for_saving, f"{self.posting_dir_path}\\posting{self.posting_file_counter}")
    #     self.posting_file_counter += 1

    def update_pointers(self):
        for term in list(self.posting_copy_for_saving.keys()):
            self.inverted_idx[term]['pointers'].append(f"{self.posting_dir_path}\\posting{self.posting_file_counter}")

    def create_postings_AtoZ(self):

        for letter in string.ascii_uppercase:
            if letter != 'Q' and letter != 'X' and letter != 'Z':  # least common letters at the beginning of a word
                utils.save_obj({}, f"{self.posting_dir_path}\\posting{letter}")
        utils.save_obj({}, f"{self.posting_dir_path}\\postingQXZ")
        utils.save_obj({}, f"{self.posting_dir_path}\\postingSPECIALS")

    def __del__(self):
        self.executor.shutdown()
