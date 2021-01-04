from spellchecker import SpellChecker


class SpellCheck:

    @staticmethod
    def spellCheck(string_to_check):
        corrected_word_string = ""
        words_to_check = string_to_check.split()
        spell = SpellChecker()

        spell.word_frequency.add("coronavirus")

        for word in words_to_check:
            corrected_word = spell.correction(word)
            corrected_word_string = corrected_word_string + " " + corrected_word

        return corrected_word_string[1:]
