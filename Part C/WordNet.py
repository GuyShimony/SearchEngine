from nltk.corpus import wordnet


class WordNet:

    @staticmethod
    def weights(words_to_check):
        query_terms = []
        query_dict = words_to_check
        for term in query_dict.keys():  # we didn't want to expand entities
            if term.lower() == "coronavirus" or term.lower() == "covid": # those are common words
                continue
            term_similar_words = wordnet.synsets(term)
            if not term_similar_words:
                continue
            synonyms_to_add = set()
            i = 0
            while i<3:
                lemma = term_similar_words[0].lemmas()[i]
                if lemma:
                    expansion_word = lemma.name()
                    if expansion_word not in query_dict and expansion_word not in synonyms_to_add:
                        synonyms_to_add.add(expansion_word)

            # for lemma in term_similar_words[0].lemmas():
            #     if lm.name() not in query_dict and lm.name() not in terms_expand:
            #         terms_expand.add(lm.name())

            query_terms += list(synonyms_to_add)






# for i, term in enumerate(query_terms):
#             if term in entities:
#                 query_wights.append(1*entities[term])
#             elif term in dict_from_pars:
#                 query_wights.append(1*dict_from_pars[term])
#             else: # from expand
#                 query_wights.append(0.2)