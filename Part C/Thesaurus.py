from nltk.corpus import lin_thesaurus as thesaurus


class Thesaurus:

    @staticmethod
    def synonyms(words_to_check):
        query_terms = []

        for word in words_to_check:
            synonym_words = thesaurus.synonyms(word)
            if not synonym_words:
                continue
            # take noun words only if exist
            noun_synonyms = synonym_words[1]
            if len(noun_synonyms[1]) > 0:
                # take highest fit noun word
                noun_word = list(noun_synonyms[1])[0]
                query_terms.append(noun_word)
        Thesaurus.add_to_dict(words_to_check, query_terms)
        return words_to_check

    @staticmethod
    def add_to_dict(query_dict, synonyms_to_add):
        # each word will be added
        # if the word already exists in the query dict it won't be sent to method , "all new words appear once"
        for synonym in synonyms_to_add:
            query_dict[synonym] = 0.2  # give new added words a smaller weight
