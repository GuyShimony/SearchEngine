import copy
from threading import Thread
import utils
import os
import shutil

class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.k = 200
        self.term_counter = 0
        self.posting_dir_path = f'{os.getcwd()}\\Postings'  # Path for posting directory - current working dir + Posting
        self.posting_file_counter = 1
        self.posting_copy_for_saving = None
        shutil.rmtree(self.posting_dir_path) # Remove at the end
        if not os.path.exists(self.posting_dir_path):
            # Create a directory for all posting files
            os.makedirs(self.posting_dir_path)

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        # Go over each term in the doc
        for term in document_dictionary.keys():
            print(self.term_counter)
            self.term_counter += 1
            if self.term_counter > self.k:
                self.term_counter = 0
                self.posting_copy_for_saving = copy.deepcopy(self.postingDict)
                Thread(target=self.update_posting_in_dict).start()
                Thread(target=self.posting_save).start()  # Start each posting file saving process in a new thread
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
                    self.postingDict[term] = {"df": 1, "tweets":[]}
                else:
                    #tuples of tweet id , number of occurrences in the tweet
                    self.postingDict[term]["tweets"].append((document.tweet_id, document_dictionary[term]))
                    #number of tweets the term appeared in
                    self.postingDict[term]['df'] += 1

            except:
                print('problem with the following key {}'.format(term[0]))

    def posting_save(self):
        utils.save_obj(self.postingDict, f"{self.posting_dir_path}\\posting{self.posting_file_counter}")
        self.posting_file_counter += 1

    def update_posting_in_dict(self):
        for term in list(self.posting_copy_for_saving.keys()):
            self.inverted_idx[term]['pointers'].append(f"{self.posting_dir_path}\\posting{self.posting_file_counter}")
