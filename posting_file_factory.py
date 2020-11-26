import string
import utils
from threading import Thread
import os
from queue import Queue


class PostingFilesFactory:
    instance = None

    def __init__(self, config):

        if PostingFilesFactory.instance is None:
            self.config = config
            self.posting_paths = {}
            self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
            PostingFilesFactory.instance = self
            #         Thread(target=self.create_postings).start()
            self.posting_files_path_counter = {}
            self.queue = Queue()

    def get_file_path(self, word):
        word = word.lower()
        if word[0] in self.posting_paths:
            return self.posting_paths[word[0]]
        else:
            return f"{self.posting_dir_path}\\postingSPECIALS"

    def get_posting_file_and_path(self, word):

        return utils.load_obj(self.get_file_path(word)), self.get_file_path(word)

    def create_postings(self):

        self.posting_paths = {
            '#': f"{self.posting_dir_path}\\posting#",
            '@': f"{self.posting_dir_path}\\posting@",
            '0': f"{self.posting_dir_path}\\postingNUM",
            '1': f"{self.posting_dir_path}\\postingNUM",
            '2': f"{self.posting_dir_path}\\postingNUM",
            '3': f"{self.posting_dir_path}\\postingNUM",
            '4': f"{self.posting_dir_path}\\postingNUM",
            '5': f"{self.posting_dir_path}\\postingNUM",
            '6': f"{self.posting_dir_path}\\postingNUM",
            '7': f"{self.posting_dir_path}\\postingNUM",
            '8': f"{self.posting_dir_path}\\postingNUM",
            '9': f"{self.posting_dir_path}\\postingNUM",
            'q': f"{self.posting_dir_path}\\postingQXZ",
            'x': f"{self.posting_dir_path}\\postingQXZ",
            'z': f"{self.posting_dir_path}\\postingQXZ"
        }
        # if not in dict --> utils.save_obj({}, f"{self.posting_dir_path}\\postingSPECIALS")
        for letter in string.ascii_lowercase:
            if letter is not self.posting_paths:
                self.posting_paths[letter] = f"{self.posting_dir_path}\\posting{letter}"
        self.posting_paths["SPECIALS"] = f"{self.posting_dir_path}\\postingSPECIALS"
        for key in self.posting_paths:
            utils.save_obj({}, self.posting_paths[key])

    def create_posting_files(self, posting_dict, letter_word_mapping):

        for letter in letter_word_mapping:
            word_data_dict = {}
            try:
                if letter.isdigit():
                    letter = 'NUM'
                    count = self.posting_files_path_counter[letter]
                elif letter not in string.ascii_letters and letter != '#' and letter != '@':
                    letter = 'SPECIALS'
                    count = self.posting_files_path_counter['SPECIALS']
                else:
                    count = self.posting_files_path_counter[letter]
            except KeyError:
                self.posting_files_path_counter[letter] = 0
                count = self.posting_files_path_counter[letter]
            finally:
                self.posting_files_path_counter[letter] += 1
                for word in letter_word_mapping[letter]:
                    word_data_dict[word] = posting_dict[word]

                utils.save_obj(word_data_dict, f"{self.posting_dir_path}\\{letter}{count}")

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
        self.queue.get() #empty the queue from last file name

    @staticmethod
    def get_instance(config):
        if not PostingFilesFactory.instance:
            PostingFilesFactory(config)
        return PostingFilesFactory.instance
