



dict_from_pars, entities, is_retweet = self.parser.parse_sentence(query)
        query_terms = list(dict_from_pars.keys()) + list(entities.keys())
        if len(query_terms) < 6:  # there is no need to expand if its more
            terms_expand = set()
            for term in dict_from_pars.keys():  # we didn't want to expand entities
                if term == "covid" or term.lower() == "trump": # those are common words
                    continue
                if len(wordnet.synsets(term)) > 0:
                    for lm in wordnet.synsets(term)[0].lemmas():
                        if lm.name() not in dict_from_pars and lm.name() not in terms_expand and not ("_" in lm.name()):
                            terms_expand.add(lm.name())

            query_terms += list(terms_expand)






for i, term in enumerate(query_terms):
            if term in entities:
                query_wights.append(1*entities[term])
            elif term in dict_from_pars:
                query_wights.append(1*dict_from_pars[term])
            else: # from expand
                query_wights.append(0.2)