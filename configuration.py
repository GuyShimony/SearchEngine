class ConfigClass:
    def __init__(self):
        self.corpusPath = ''
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

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
