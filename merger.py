import os
from queue import Queue
import utils
from queue import Queue


class Merger:

    def __init__(self, path_to_files, file_type):
        # Queue.__init__(self)
        self.queue = Queue()
        self.p1 = None
        self.p2 = None
        self.start_index = 0
        self.files_name_pattern = None
        self.file_type = file_type
        self.path_to_files = path_to_files

    def merge(self, group_id):
        if not os.listdir(self.path_to_files):
            # directory is empty
            return

        self.files_name_pattern = group_id
        self.collect_files()

        while self.queue.qsize() > 1:
            self.p1 = self.queue.get().replace("." + self.file_type, "")
            self.p2 = self.queue.get().replace("." + self.file_type, "")
            posting_1 = utils.load_obj(f"{self.path_to_files}\\{self.p1}")
            posting_2 = utils.load_obj(f"{self.path_to_files}\\{self.p2}")
            # merge the 2 dictionaries
            posting_3 = {**posting_1, **posting_2}
            for key, value in posting_3.items():
                if key in posting_1 and key in posting_2:  # if 2 keys were similar 3 got 2's keys
                    posting_3[key]['docs'] = posting_2[key]['docs'] + posting_1[key]['docs']
                    posting_3[key]['df'] = posting_2[key]['df'] + posting_1[key]['df']

            combine = f"{self.p1}_{self.p2}"
            # if combine[-1:] == "0":  # Use to prevent to long file name. Every 10 files reset the name
            #     combine = f"{self.files_name_pattern}0"
            utils.save_obj(posting_3, f"{self.path_to_files}\\{combine}")
            os.remove(f"{self.path_to_files}\\{self.p1}.{self.file_type}")
            os.remove(f"{self.path_to_files}\\{self.p2}.{self.file_type}")
            self.queue.put(combine)

        final_name = self.queue.get().replace("." + self.file_type, "") # empty the queue from last file name
        #  Change the name of the last file to 'a.pkl' instead of 'a0_a1_.._ak'
        os.rename(f"{self.path_to_files}\\{final_name}.{self.file_type}",
                  f"{self.path_to_files}\\posting{self.files_name_pattern}.{self.file_type}")

    def collect_files(self):
        for file in sorted(os.listdir(self.path_to_files)):
            self.queue.put(file)
