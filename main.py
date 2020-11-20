import search_engine
import parser_module

if __name__ == '__main__':

    #search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("#HelloBitch #heyThere 1000 Million"
                          ,False)
