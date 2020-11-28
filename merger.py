import math
import os
import utils
from queue import Queue


class Merger:

    def __init__(self, path_to_files, file_type, docs_file, corpus_size=0):

        self.corpus_size = corpus_size
        self.queue = Queue()
        self.dict1 = None
        self.dict2 = None
        self.files_name = None
        self.file_type = file_type
        self.path_to_files = path_to_files
        self.docs_file = docs_file

    def merge(self, group_id):
        was_combined = False
        merged_dict = None
        self.files_name = group_id
        self.collect_files()

        while self.queue.qsize() > 1:
            if not was_combined:
                was_combined = True
            self.dict1 = self.queue.get()
            self.dict2 = self.queue.get()
            # merge the 2 dictionaries
            merged_dict = {**self.dict1, **self.dict2}
            for key, value in merged_dict.items():
                if key in self.dict1 and key in self.dict2:  # if 2 keys were similar 3 got 2's keys
                    merged_dict[key]['docs'] = value['docs'] + self.dict1[key]['docs']
                    merged_dict[key]['df'] = value['df'] + self.dict1[key]['df']
                self.calculate_doc_weight(merged_dict, key)

            self.queue.put(merged_dict)

        if was_combined:
            utils.save_obj(merged_dict, f"{self.path_to_files}\\{self.files_name}")
        else:
            file_dict = self.queue.get()
            for key in file_dict:
                self.calculate_doc_weight(file_dict, key)
            utils.save_obj(file_dict, f"{self.path_to_files}\\{self.files_name}")

    def collect_files(self):
        #for file in sorted(os.listdir(self.path_to_files)):
        file_handle = utils.open_file(f"{self.path_to_files}\\{self.files_name}")
        obj = utils.get_next(file_handle)
        while obj:
            self.queue.put(obj)
            obj = utils.get_next(file_handle)
        utils.close_file(file_handle)

    def calculate_doc_weight(self, merged_dict, key):
        for i in range(len(merged_dict[key]['docs'])):
            term_tf = merged_dict[key]['docs'][i][1]
            doc_len = merged_dict[key]['docs'][i][2]
            term_df = merged_dict[key]['df']
            doc_id = merged_dict[key]['docs'][i][0]
            max_tf = self.docs_file[doc_id][1]
            term_idf = math.log10(self.corpus_size / term_df)
            # save term's idf
            merged_dict[key]['idf'] = term_idf
            # calculate doc's total weight
            self.docs_file[doc_id][0] += 0.6 * (term_tf / max_tf) * term_idf + 0.4 * (term_tf / doc_len) * term_idf