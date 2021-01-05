from reader import ReadFile
from parser_module import Parse
import os
import string

preprocessed_file = "preprocessoed.txt"
reader = ReadFile(r"C:\Users\Owner\Desktop\SearchEngine\Data\date=07-15-2020")
parser = Parse()

documents_list = reader.read_file("covid19_07-15.snappy.parquet")

if os.path.exists(preprocessed_file):
    os.remove(preprocessed_file)

with open(preprocessed_file, "a+") as f:
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = parser.parse_doc(document)
        doc = ""
        for i, word in enumerate(parsed_document.term_doc_dictionary):
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
            print(doc)
            f.write(doc)
