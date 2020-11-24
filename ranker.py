import math


class Ranker:
    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_doc(relevant_docs, number_of_documents):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        document_scores = Ranker.tf_idf(relevant_docs, number_of_documents)

        return sorted(document_scores.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def tf_idf(relevant_docs, number_of_documents):

        document_scores = {}
        # return sorted(relevant_doc.items(), key=lambda item: item[1], reverse=True)
        for document_id in relevant_docs:
            score = 0
            for term_tf, term_df in zip(relevant_docs[document_id][1], relevant_docs[document_id][4]):
                score += (term_tf / relevant_docs[document_id][2]) * math.log10(
                    number_of_documents / term_df)  # tfi * idf
                document_scores[document_id] = score
        # return sorted(document_scores.items(), key=lambda item: item[1], reverse=True)
        return document_scores

    @staticmethod
    def cosine_sim(relevant_docs, number_of_documents):
        # numerator -> inner product
        document_weights = Ranker.tf_idf(relevant_docs, number_of_documents)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
