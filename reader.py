import os
import pandas as pd


class ReadFile:
    def __init__(self, corpus_path):
        # self.corpus_path = f"{os.getcwd()}\\{corpus_path}"
        self.corpus_path = corpus_path

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read or a directory that contains
        multiple parquet files in other directories
        :return: a dataframe contains tweets.
        """
        parquet_files = []
        # full_path = os.path.join(self.corpus_path, file_name)
        full_path = self.corpus_path

        if ".parquet" in file_name:  # Load the single file given
            df = pd.read_parquet(full_path, engine="pyarrow")

        else:  # Read all .parquet files from the directory given
            for root, dirs, files in os.walk(full_path):
                for file in files:
                    if file.endswith(".parquet"):
                        full_path = os.path.join(root, file)
                        df = pd.read_parquet(full_path, engine="pyarrow")
                        parquet_files.append(df)

            df = pd.concat(parquet_files, sort=False)

        return df.values.tolist()


