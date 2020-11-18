import search_engine
import parser_module

if __name__ == '__main__':

    search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("Donald Trump is a Donald Trump. hello 123 thousand, March 2009 #seayousoon #CULTforGOOD 1000 Million"
                          ,False)
