import search_engine
import reader as r
import parser_module

if __name__ == '__main__':

 #   reader = r.ReadFile("Data")
#    reader.read_file("covid19_07-09.snappy.parquet")
    search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("Donald Trump Donald Trump hello 123 thousand March 2009 #seayousoon #CULTforGOOD 1000 Million"
                          ,False)
