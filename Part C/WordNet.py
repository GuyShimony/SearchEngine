from nltk.corpus import wordnet


class WordNet:
    """
    The class is used to expand the query with related words using the wordnet module from nltk
    """

    @staticmethod
    def Expand(query_dict):
        """
        The method will receive a query after parse, expand it by adding a maximum of 2 related words using the wordnet module
        and return the updated query dictionary
        :param query_dict: a parsed query {term: tf in dictionary}
        :return: updated query dictionary with added terms
        """

        query_terms = []
        for term in query_dict.keys():
            if term.lower() == "coronavirus" or term.lower() == "covid":  # those are common words
                continue
            term_similar_words = wordnet.synsets(term)
            if not term_similar_words:
                continue
            synonyms_to_add = set()
            i = 0
            lemma = term_similar_words[0].lemmas()
            lemma_len = len(lemma)
            # maximum of 3 expansion words will be added to the query
            while i < 2 and i < lemma_len:
                lemma_type = lemma[i]
                if lemma_type:
                    expansion_word = lemma_type.name()
                    if expansion_word not in query_dict and expansion_word not in synonyms_to_add:
                        synonyms_to_add.add(expansion_word)
                i += 1

            query_terms += list(synonyms_to_add)
        WordNet.add_to_dict(query_dict, query_terms)
        return query_dict

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
            query_dict[synonym] = 0.2 #give new added words a smaller weight
