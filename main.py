import search_engine
import parser_module
import os
if __name__ == '__main__':
    #search_engine.main(f"{os.getcwd()}\\samples", f"{os.getcwd()}\\Postings", False, "queries.txt", 5)
    parser = parser_module.Parse()
    parser.parse_sentence('#AreYouOk 20 covid-19 1290538171420130000 tweets 40 thousand percent 50 kids 600 thousand children 300 percentage 40000 percent')
