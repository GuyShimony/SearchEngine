import os


class ConfigClass:
    def __init__(self):
        # link to a zip file in google drive with your pretrained model
        self._model_url = "https://drive.google.com/file/d/14w0Cc4oMyMqUDs25TI1eP1V95LGgPX9l/view?usp=sharing"
        # False/True flag indicating whether the testing system will download 
        # and overwrite the existing model files. In other words, keep this as 
        # False until you update the model, submit with True to download 
        # the updated model (with a valid model_url), then turn back to False 
        # in subsequent submissions to avoid the slow downloading of the large 
        # model file with every submission.
        self._download_model = False
        self._model_dir = None

        self.corpusPath = ''
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False
        self.toLemm = False

        # Used for tests
        self.google_news_vectors_negative300_path = '../../../../GoogleNews-vectors-negative300.bin'
        self.glove_twitter_27B_25d_path = '../../../../glove.twitter.27B.25d.txt'

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_model_url(self):
        return self._model_url

    def get_download_model(self):
        return self._download_model

    def set_output_path(self, path):
        self.savedFileMainFolder = path
        if 'nt' in os.name:  # Windows system
            self.saveFilesWithStem = self.savedFileMainFolder + "\\WithStem"
            self.saveFilesWithoutStem = self.savedFileMainFolder + "\\WithoutStem"
        else:  # Unix system
            self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
            self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"

    def get_output_path(self):
        """
        Get the output for the posting files
        """
        return os.getcwd()

    @property
    def model_dir(self):
        return self._model_dir

    @model_dir.setter
    def model_dir(self, model_dir):
        self._model_dir = model_dir
