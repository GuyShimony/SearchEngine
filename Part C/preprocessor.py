from reader import ReadFile
from parser_module import Parse
from stemmer import Stemmer
import os
import string

preprocessed_file = "model/preprocessed.txt"
corpus_path = r"C:\Users\Owner\Desktop\SearchEngine\Data"
reader = ReadFile(corpus_path)
parser = Parse()
stemmer = Stemmer()
#documents_list = reader.read_file("covid19_08-05.snappy.parquet")

documents_list = []
files_to_process = [
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-08-2020\covid19_07-08.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-09-2020\covid19_07-09.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-10-2020\covid19_07-10.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-11-2020\covid19_07-11.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-12-2020\covid19_07-12.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-13-2020\covid19_07-13.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-15-2020\covid19_07-15.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-16-2020\covid19_07-16.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-17-2020\covid19_07-17.snappy.parquet",
    r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-18-2020\covid19_07-18.snappy.parquet",
]

for file in files_to_process:
    documents_list += reader.read_file(file)

with open(preprocessed_file, "a+") as f:
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = parser.parse_doc(document)
        doc = ""
        for i, word in enumerate(parsed_document.term_doc_dictionary):
            if word == "#" or "#_" in word:
                continue

            if i == len(parsed_document.term_doc_dictionary) - 1:
                doc = doc.replace('\n', "")
                doc+="\n"
                break

            doc+= f"{word} "
        try:
            f.write(doc)
        except UnicodeEncodeError:
            for char in doc:
                if char not in string.printable:
                    doc = doc.replace(char, "")

                if word == "#" or "#_" in word:
                    continue

            f.write(doc)
