import search_engine
import reader as r
import parser_module

if __name__ == '__main__':

 #   reader = r.ReadFile("Data")
#    reader.read_file("covid19_07-09.snappy.parquet")
   # search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("USA Hello You stupid, 3.87 34 3/4 12345% 1234567890, hello 123 Thousand You 15 million @fucker #smartAss ; https://www.instagram.co.il/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x")
