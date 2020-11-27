import string
import time

import utils
from threading import Thread
import os
from queue import Queue
from merger import Merger
from  concurrent.futures import ThreadPoolExecutor


class PostingFilesFactory:
    instance = None

    def __init__(self, config):

        if PostingFilesFactory.instance is None:
            self.config = config

            self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
            PostingFilesFactory.instance = self

            self.posting_paths = {
                '#': {"file": "posting#", "path": f"{self.posting_dir_path}\\Dir_#", "name": "#"},
                '@': {"file": "posting@", "path": f"{self.posting_dir_path}\\Dir_@", "name": "@"},
                '0': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '1': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '2': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '3': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '4': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '5': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '6': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '7': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '8': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                '9': {"file": "postingNUM", "path": f"{self.posting_dir_path}\\Dir_num", "name": "NUM"},
                'q': {"file": "postingQXZ", "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'x': {"file": "postingQXZ", "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'z': {"file": "postingQXZ", "path": f"{self.posting_dir_path}\\Dir_qxz", "name": "QXZ"},
                'SPECIALS': {"file": "postingSPECIALS", "path": f"{self.posting_dir_path}\\Dir_specials", "name": "SPECIALS"}
            }
            self.create_postings_dirs()
            self.posting_files_path_counter = {}
            self.executer = None
            self.queue = Queue()

    def get_file_path(self, word):
        word = word.lower()
        if word[0] in self.posting_paths:
            return f"{self.posting_paths[word[0]]['path']}\\{self.posting_paths[word[0]]['file']}"
        else:
            return f"{self.posting_dir_path}\\Dir_specials\\postingSPECIALS"

    def get_posting_file_and_path(self, word):

        return utils.load_obj(self.get_file_path(word)), self.get_file_path(word)

    def create_postings_dirs(self):

        # self.posting_paths = {
        #     '#': f"{self.posting_dir_path}\\posting#",
        #     '@': f"{self.posting_dir_path}\\posting@",
        #     '0': f"{self.posting_dir_path}\\postingNUM",
        #     '1': f"{self.posting_dir_path}\\postingNUM",
        #     '2': f"{self.posting_dir_path}\\postingNUM",
        #     '3': f"{self.posting_dir_path}\\postingNUM",
        #     '4': f"{self.posting_dir_path}\\postingNUM",
        #     '5': f"{self.posting_dir_path}\\postingNUM",
        #     '6': f"{self.posting_dir_path}\\postingNUM",
        #     '7': f"{self.posting_dir_path}\\postingNUM",
        #     '8': f"{self.posting_dir_path}\\postingNUM",
        #     '9': f"{self.posting_dir_path}\\postingNUM",
        #     'q': f"{self.posting_dir_path}\\postingQXZ",
        #     'x': f"{self.posting_dir_path}\\postingQXZ",
        #     'z': f"{self.posting_dir_path}\\postingQXZ"
        # }

        # if not in dict --> utils.save_obj({}, f"{self.posting_dir_path}\\postingSPECIALS")
        for letter in string.ascii_lowercase:
            if self.posting_paths.get(letter) is None:
                self.posting_paths[letter] = {"file": f"posting{letter}",
                                              "path": f"{self.posting_dir_path}\\Dir_{letter}",
                                              "name": letter}

        self.posting_paths["SPECIALS"] = {"file": "postingSPECIALS}",
                                          "path": f"{self.posting_dir_path}\\Dir_specials",
                                          "name": "SPECIALS"}
        for key in self.posting_paths:
            if not os.path.exists(self.posting_paths[key]['path']):
                # utils.save_obj({}, self.posting_paths[key])
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
                # if char.isdigit():
                #     char = 'NUM'
                #     count = self.posting_files_path_counter[char]
                # elif char not in string.ascii_letters and char != '#' and char != '@':
                #     char = 'SPECIALS'
                #     count = self.posting_files_path_counter['SPECIALS']
                # else:
                #     count = self.posting_files_path_counter[char]
            except KeyError:
                self.posting_files_path_counter[name] = 0
                count = self.posting_files_path_counter[name]
            finally:
                self.posting_files_path_counter[name] += 1
                for word in letter_word_mapping[char]:
                    word_data_dict[word] = posting_dict[word]

            utils.save_obj(word_data_dict, f"{self.posting_paths[char_path]['path']}\\{name}{count}")

    def merge_file_group(self, group_id):
        for index in range(self.posting_files_path_counter[group_id]):
            self.queue.put(group_id + f"{index}")
        while self.queue.qsize() > 1:
            filename_1 = self.queue.get()
            filename_2 = self.queue.get()
            posting_1 = utils.load_obj(f"{self.posting_dir_path}\\{filename_1}")
            posting_2 = utils.load_obj(f"{self.posting_dir_path}\\{filename_2}")
            # merge the 2 dictionaries
            posting_3 = {**posting_1, **posting_2}
            for key, value in posting_3.items():
                if key in posting_1 and key in posting_2:  # if 2 keys were similar 3 got 2's keys
                    posting_3[key] = value + posting_1[key]
            if self.queue.qsize() == 1:
                filename_3 = group_id
            else:
                filename_3 = filename_1 + filename_2
            utils.save_obj(posting_3, f"{self.posting_dir_path}\\{filename_3}")
            os.remove(f"{self.posting_dir_path}\\{filename_1}.pkl")
            os.remove(f"{self.posting_dir_path}\\{filename_2}.pkl")
            self.queue.put(filename_3)
        self.queue.get()  # empty the queue from last file name

    @staticmethod
    def get_instance(config):
        if not PostingFilesFactory.instance:
            PostingFilesFactory(config)
        return PostingFilesFactory.instance

    def set_threadpool(self, threadpool_executor):
        self.executer = threadpool_executor

    def merge(self):
        for key in self.posting_paths:  # directory is empty
            if os.listdir(self.posting_paths[key]['path']):
                merger = Merger(self.posting_paths[key]['path'], "pkl")
                merger.merge(self.posting_paths[key]['name'])
                # if key == "q" or key == "x" or key =="z":
                #     if not flag:
                #         merger = Merger(self.posting_paths[key]['path'], "pkl")
                #         threads.append(Thread(target=merger.merge, args=(self.posting_paths[key]['name'],)))
                #         flag = True
                # else:
                #     merger = Merger(self.posting_paths[key]['path'], "pkl")
                #     threads.append(Thread(target=merger.merge, args=(self.posting_paths[key]['name'],)))
                # # with ThreadPoolExecutor(max_workers=10) as executor:
                # # executor.submit(merger.merge, self.posting_paths[key]['name'])
                # # executor.shutdown()
        #
        # for thread in threads:
        #     thread.start()
        #
        # return threads
        # for thread in threads:
        #     thread.join()
                # if self.executer:  # Run each merger parallel
                #     self.executer.submit(merger.merge, self.posting_paths[key]['name'])
                # else:
                #     merger.merge(self.posting_paths[key]['name'])
