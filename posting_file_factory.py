import os
import string
from queue import Queue

import utils
from merger import Merger


class PostingFilesFactory:
    instance = None

    def __init__(self, config):

        if PostingFilesFactory.instance is None:
            self.config = config

            self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
            PostingFilesFactory.instance = self

            self.postings_data = {
                '#': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_#", "name": "#"},
                '@': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_@", "name": "@"},
                '0': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_0", "name": "0"},
                '1': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_1", "name": "1"},
                '2': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_2", "name": "2"},
                '3': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_3", "name": "3"},
                '4': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_4", "name": "4"},
                '5': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_5", "name": "5"},
                '6': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_6", "name": "6"},
                '7': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_7", "name": "7"},
                '8': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_8", "name": "8"},
                '9': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_9", "name": "9"},
                'q': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'x': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'z': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'SPECIALS': {"counter": 0, "path": f"{self.posting_dir_path}\\Dir_specials", "name": "SPECIALS"}
            }
            self.create_postings_dirs()
            self.queue = Queue()

    def get_file_path(self, word):
        word = word.lower()
        if word[0] in self.postings_data:
            return f"{self.postings_data[word[0]]['path']}\\{self.postings_data[word[0]]['name']}"
        else:
            return f"{self.posting_dir_path}\\Dir_specials\\SPECIALS"

    def get_posting_file_and_path(self, word):
        return utils.load_obj(self.get_file_path(word)), self.get_file_path(word)

    def get_docs_file(self):
        return utils.load_obj(f"{self.posting_dir_path}\\docs\\docs_index")

    def save_docs_file(self, file):
        return utils.save_obj(file, f"{self.posting_dir_path}\\docs\\docs_index")

    def create_postings_dirs(self):

        # if not in dict --> utils.save_obj({}, f"{self.posting_dir_path}\\postingSPECIALS")
        for letter in string.ascii_lowercase:
            if self.postings_data.get(letter) is None:
                self.postings_data[letter] = {"counter": 0,
                                              "path": f"{self.posting_dir_path}\\Dir_{letter}",
                                              "name": letter}

        for key in self.postings_data:
            if not os.path.exists(self.postings_data[key]['path']):
                os.makedirs(self.postings_data[key]['path'])

    def create_posting_files(self, posting_dict, letter_word_mapping):
        for char in letter_word_mapping:
            word_data_dict = {}
            if self.postings_data.get(char) is None:
                name = "SPECIALS"
                char_path = "SPECIALS"
            else:
                name = self.postings_data[char]['name']
                char_path = char
            for word in letter_word_mapping[char]:
                word_data_dict[word] = posting_dict[word]

            if word_data_dict:
                utils.append(word_data_dict, f"{self.postings_data[char_path]['path']}\\{name}")

    @staticmethod
    def get_instance(config):
        if not PostingFilesFactory.instance:
            PostingFilesFactory(config)
        return PostingFilesFactory.instance

    def merge(self, corpus_size):
        """
        The function will merge all the data in the posting files using the BSBI algorithm
        """
        docs_file = self.get_docs_file()
        for key in self.postings_data:
            if os.listdir(self.postings_data[key]['path']):  # directory is not empty
                merger = Merger(self.postings_data[key]['path'], "pkl", docs_file, corpus_size)
                merger.merge(self.postings_data[key]['name'])

        #  The merger updates the docs data. After the merge of all the letters - all the documents data
        #  Is updated and need to be saved on disk to reduce the memory load
        utils.save_obj(docs_file, f"{self.posting_dir_path}\\docs\\docs_index")
