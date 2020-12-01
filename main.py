import search_engine
import parser_module
import os
if __name__ == '__main__':
    search_engine.main(f"{os.getcwd()}\\Data", f"{os.getcwd()}\\Postings", True, "test\\queries_new.txt", 5)
    parser = parser_module.Parse()
    parser.parse_sentence('RT 40 childrens 20 percentage 10 thousand 2% @AncelottiMagic: I’d be pissed off if I was...')
