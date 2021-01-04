import math


# you can change whatever you want in this module, just make sure it doesn't
# break the searcher module
class Ranker:
    query_terms = {}
    query_weight = 0
    avdl = 1

    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_docs(relevant_docs, corpus_size, k=None):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param number_of_documents: total docs in corpus
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        total_doc_scores = {}
        for rel_doc in relevant_docs:
            total_doc_scores[rel_doc] = 0
        document_scores_cosin = Ranker.cosine_sim(relevant_docs)
        document_scores_BM25 = Ranker.BM25(relevant_docs, corpus_size)
        for doc in total_doc_scores:
            inner_product_score = Ranker.inner_product(doc)
          #  total_doc_scores[doc] = 0.8 * document_scores_cosin[doc] + 0.2 * inner_product_score
            total_doc_scores[doc] = 0.6 * document_scores_BM25[doc] + 0.2 * document_scores_cosin[doc] + 0.2 * inner_product_score
        return sorted(total_doc_scores.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def tf_idf(relevant_docs, number_of_documents):

        document_scores = {}
        for document_id in relevant_docs:
            score = 0
            for term_tf, term_df in zip(relevant_docs[document_id][6], relevant_docs[document_id][7]):
                score += term_tf * math.log10(number_of_documents / term_df)  # tfi * idf
                document_scores[document_id] = score
        return document_scores

    @staticmethod
    def inner_product(relevant_doc):
        inner_product = 0
        for query_term in Ranker.query_terms:
            if query_term in relevant_doc[1]:
                term_index = relevant_doc[1].index(query_term)
                term_weight_doc = relevant_doc[5][term_index]
                term_weight_query = Ranker.query_terms[query_term]
                inner_product += term_weight_doc * term_weight_query
        return inner_product

    @staticmethod
    def cosine_sim(relevant_docs):
        document_scores_cosin = {}
        # numerator -> inner product
        for relevant_doc in relevant_docs:
            inner_product = 0
            for term_index in range(len(relevant_docs[relevant_doc][1])):
                term = relevant_docs[relevant_doc][1][term_index]
                term_weight_doc = relevant_docs[relevant_doc][5][term_index]
                term_weight_query = Ranker.query_terms[term]
                inner_product += term_weight_doc * term_weight_query

            # denominator left -> term per doc weight squared
            doc_weight = relevant_docs[relevant_doc][8]
            # denominator right -> term per query weight squared

            cosin_denominator = math.sqrt(doc_weight * Ranker.query_weight)
            cosin_score = inner_product / cosin_denominator
            document_scores_cosin[relevant_doc] = cosin_score

        return document_scores_cosin

    @staticmethod
    def BM25(relevant_docs, corpus_size, k=1.5, b=0.75):
        # common terms for query and each document are found in the docs_idx[1]
        document_scores_BM25 = {}
        for doc_id in relevant_docs:
            common_terms = relevant_docs[doc_id][1]
            common_terms_tf = relevant_docs[doc_id][6]
            common_terms_df = relevant_docs[doc_id][7]

            doc_score = 0
            doc_len = relevant_docs[doc_id][3]

            for index, term in enumerate(common_terms):
                term_tf = common_terms_tf[index]
                term_df = common_terms_df[index]
                term_idf = math.log2(corpus_size / term_df)
                numerator = term_tf * (term_tf * (k + 1))
                denominator = term_tf #+ (k * (1 - b + (b * doc_len / Ranker.avdl)))
                doc_score += (term_idf * (numerator / denominator))
            document_scores_BM25[doc_id] = doc_score
        return document_scores_BM25

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
