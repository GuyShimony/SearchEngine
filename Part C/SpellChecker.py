from spellchecker import SpellChecker


class SpellCheck:

    @staticmethod
    def spellCheck(words_to_check):
        corrected_words_to_check = {}
        spell = SpellChecker()
        spell.word_frequency.add("coronavirus")

        for word in words_to_check:
            corrected_word = spell.correction(word)
            corrected_words_to_check[corrected_word] = words_to_check[word]
        #   corrected_word_string = corrected_word_string + " " + corrected_word

        # return corrected_word_string[1:]
        return corrected_words_to_check
