from thesaurus_api import *
from py_thesaurus import Thesaurus


class Thesaurus1:

    @staticmethod
    def synonyms(string_to_evaluate):

        new_instance = Thesaurus('young')
        mor = new_instance.get_synonym()
        synonym_word_string = ""
        words_to_evaluate = string_to_evaluate.split()
        check = Word('box')
        check1 = check.synonyms()
        for word in words_to_evaluate:
            word_def = Word(word)
            synonym_word = word_def.synonyms()
            if synonym_word:
                print("DGDFG")
                synonym_word_string = synonym_word_string + " " + synonym_word
            else:
                synonym_word_string = " " + string_to_evaluate
        return synonym_word_string[1:]
