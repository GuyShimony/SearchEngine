import search_engine
import parser_module
import os
if __name__ == '__main__':
    #search_engine.main(f"{os.getcwd()}", f"{os.getcwd()}\\Postings", False, "queries.txt", 3)
    parser = parser_module.Parse()
    parser.parse_sentence('#HG 40 children 50 thousand kids')
