import search_engine
import reader as r
import parser_module

if __name__ == '__main__':

 #   reader = r.ReadFile("Data")
#    reader.read_file("covid19_07-09.snappy.parquet")
   # search_engine.main()
    parser = parser_module.Parse()
    parser.parse_sentence("U.S.A HELLO You stupid, 12345% 1234567890, 123 Thousand  15 People @fucker #smartass ; https://www.instagram.co.il/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x")
