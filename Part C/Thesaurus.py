from nltk.corpus import lin_thesaurus as thesaurus


class Thesaurus:
    """
    The class is used to expand the query with synonyms using the thesaurus module from nltk
    """

    @staticmethod
    def synonyms(words_to_check):
        """
        The method will receive a query after parse, expand it by adding a noun synonym (if exists)
        to each word in the query using the lin_thesaurus module  and return the updated query dictionary
        :param words_to_check: a parsed query {term: tf in dictionary}
        :return: updated query dictionary with added terms
        """
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
        """
        :param query_dict: query_dict: a parsed query {term: tf in dictionary}
        :param synonyms_to_add: all words fetched from the wordnet module
        :return: query dictionary query_dict will be updated with added terms
        """
        # each word will be added
        # if the word already exists in the query dict it won't be sent to method , "all new words appear once"
        for synonym in synonyms_to_add:
            query_dict[synonym] = 0.2  # give new added words a smaller weight
