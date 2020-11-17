import search_engine
import reader as r
import parser_module

if __name__ == '__main__':

 #   reader = r.ReadFile("Data")
#    reader.read_file("covid19_07-09.snappy.parquet")
    #search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("March 2020 123 march 2030 mar 99 April 1994 JUN 99 dec 75 14 percent 16% guy 125 thousands"
                          ,False)
