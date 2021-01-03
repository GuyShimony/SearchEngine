import os


class ConfigClass:
    def __init__(self):

        self.corpusPath = ''
        self.savedFileMainFolder = ''
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.toStem = False

    def get__corpusPath(self):
        """
        Get the corpus path to read the data from
        """
        return self.corpusPath

    def get_output_path(self):
        """
        Get the output for the posting files
        """
        return self.saveFilesWithStem if self.toStem else self.saveFilesWithoutStem

    def set_output_path(self, path):
        self.savedFileMainFolder = path
        if 'nt' in os.name:  # Windows system
            self.saveFilesWithStem = self.savedFileMainFolder + "\\WithStem"
            self.saveFilesWithoutStem = self.savedFileMainFolder + "\\WithoutStem"
        else:  # Unix system
            self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
            self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
