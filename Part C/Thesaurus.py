from thesaurus_api import Word


class Thesaurus:

    @staticmethod
    def synonyms(string_to_evaluate):
        synonym_word_string = ""
        words_to_evaluate = string_to_evaluate.split()

        for word in words_to_evaluate:
            word_def = Word(word)
            synonym_word = word_def.synonyms(word_def)
            synonym_word_string = synonym_word_string + " " + synonym_word

        return synonym_word_string[1:]
