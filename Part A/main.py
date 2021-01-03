import search_engine
import parser_module
import os
if __name__ == '__main__':
    search_engine.main(f"{os.getcwd()}\\Data", f"{os.getcwd()}\\Postings", False, "queries.txt", 2000)
    #parser = parser_module.Parse()
    #parser.parse_sentence('https://www.instagram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x https://twitter.com/i/web/status/1290533420381085697')
