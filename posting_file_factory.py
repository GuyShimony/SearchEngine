import string
import time

import utils
from threading import Thread
import os
from queue import Queue
from merger import Merger
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool


class PostingFilesFactory:
    instance = None

    def __init__(self, config):

        if PostingFilesFactory.instance is None:
            self.config = config

            self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
            PostingFilesFactory.instance = self

            self.posting_paths = {
                '#': {"path": f"{self.posting_dir_path}\\Dir_#", "name": "#"},
                '@': {"path": f"{self.posting_dir_path}\\Dir_@", "name": "@"},
                '0': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '1': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '2': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '3': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '4': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '5': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '6': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '7': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '8': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '9': {"path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                'q': {"path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'x': {"path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'z': {"path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'SPECIALS': {"path": f"{self.posting_dir_path}\\Dir_specials", "name": "SPECIALS"}
            }
            self.create_postings_dirs()
            self.posting_files_path_counter = {}
            self.queue = Queue()

    def get_file_path(self, word):
        word = word.lower()
        if word[0] in self.posting_paths:
            return f"{self.posting_paths[word[0]]['path']}\\{self.posting_paths[word[0]]['name']}"
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
            if self.posting_paths.get(letter) is None:
                self.posting_paths[letter] = {"path": f"{self.posting_dir_path}\\Dir_{letter}",
                                              "name": letter}

        self.posting_paths["SPECIALS"] = {"path": f"{self.posting_dir_path}\\Dir_specials",
                                          "name": "SPECIALS"}
        for key in self.posting_paths:
            if not os.path.exists(self.posting_paths[key]['path']):
                os.makedirs(self.posting_paths[key]['path'])

    def create_posting_files(self, posting_dict, letter_word_mapping):
        for char in letter_word_mapping:
            word_data_dict = {}
            if self.posting_paths.get(char) is None:
                name = "SPECIALS"
                char_path = "SPECIALS"
            else:
                name = self.posting_paths[char]['name']
                char_path = char
            try:
                count = self.posting_files_path_counter[name]
            except KeyError:
                self.posting_files_path_counter[name] = 0
                count = self.posting_files_path_counter[name]
            finally:
                self.posting_files_path_counter[name] += 1
                for word in letter_word_mapping[char]:
                    word_data_dict[word] = posting_dict[word]
            if word_data_dict:
                utils.append(word_data_dict, f"{self.posting_paths[char_path]['path']}\\{name}")
                # utils.save_obj(word_data_dict, f"{self.posting_paths[char_path]['path']}\\{name}{count}")

    # def merge_file_group(self, group_id):
    #     for index in range(self.posting_files_path_counter[group_id]):
    #         self.queue.put(group_id + f"{index}")
    #     while self.queue.qsize() > 1:
    #         filename_1 = self.queue.get()
    #         filename_2 = self.queue.get()
    #         posting_1 = utils.load_obj(f"{self.posting_dir_path}\\{filename_1}")
    #         posting_2 = utils.load_obj(f"{self.posting_dir_path}\\{filename_2}")
    #         # merge the 2 dictionaries
    #         posting_3 = {**posting_1, **posting_2}
    #         for key, value in posting_3.items():
    #             if key in posting_1 and key in posting_2:  # if 2 keys were similar 3 got 2's keys
    #                 posting_3[key] = value + posting_1[key]
    #         if self.queue.qsize() == 1:
    #             filename_3 = group_id
    #         else:
    #             filename_3 = filename_1 + filename_2
    #         utils.save_obj(posting_3, f"{self.posting_dir_path}\\{filename_3}")
    #         os.remove(f"{self.posting_dir_path}\\{filename_1}.pkl")
    #         os.remove(f"{self.posting_dir_path}\\{filename_2}.pkl")
    #         self.queue.put(filename_3)
    #     self.queue.get()  # empty the queue from last file name

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
        for key in self.posting_paths:
            if os.listdir(self.posting_paths[key]['path']):  # directory is not empty
                merger = Merger(self.posting_paths[key]['path'], "pkl", docs_file, corpus_size)
                merger.merge(self.posting_paths[key]['name'])

        #  The merger udpates the docs data. After the merge of all the letters - all the documents data
        #  Is updated and need to be saved on disk to reduce the memory load
        utils.save_obj(docs_file, f"{self.posting_dir_path}\\docs\\docs_index")
