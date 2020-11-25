import string
import utils
from threading import Thread

class PostingFilesFactory:
    instance = None

    def __init__(self, config):

        if PostingFilesFactory.instance is None:
            self.config = config
            self.posting_paths = {}
            self.posting_dir_path = self.config.get_output_path()  # Path for posting directory that was given at runtime
            PostingFilesFactory.instance = self
            Thread(target=self.create_postings).start()


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

    @staticmethod
    def get_instance(config):
        if not PostingFilesFactory.instance:
            PostingFilesFactory(config)
        return PostingFilesFactory.instance
