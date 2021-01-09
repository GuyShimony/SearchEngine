from spellchecker import SpellChecker


class SpellCheck:
    """
    The class is used to correct the query's misspelled terms using the SpellChecker module
    """

    @staticmethod
    def spellCheck(words_to_check):
        """
        The method will receive a query after parse, check each term and correct its spelling if a mistake is found
        :param words_to_check: a parsed query {term: tf in dictionary}
        :return: updated query dictionary with correctly spelled terms
        """
        corrected_words_to_check = {}
        spell = SpellChecker()
        spell.word_frequency.add("coronavirus")

        for word in words_to_check:
            corrected_word = spell.correction(word)
            corrected_words_to_check[corrected_word] = words_to_check[word]

        return corrected_words_to_check
