import math
from scipy import spatial, stats
import numpy as np


# you can change whatever you want in this module, just make sure it doesn't
# break the searcher module
class Ranker:
    query_terms = {}
    query_weight = 0
    avdl = 1
    query_vector = None
    max_tfidf_score = 0

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

        #  CALCULATE EACH DOC SCORE ACCORDING TO WEIGHTS ON ALL THE SIM FUNCTION: INNER, COSINE, BM25
        for doc in relevant_docs:
            #  cosine_score = Ranker.cosine_doc_score(relevant_docs[doc])
            BM25_score = Ranker.BM25_doc_score(relevant_docs[doc], corpus_size)
            inner_product_score = Ranker.inner_product(doc)
            total_doc_scores[doc] = 0.1 * BM25_score + 0.9 * inner_product_score  # + 0 * cosine_score
            if total_doc_scores[doc] > Ranker.max_tfidf_score:
                Ranker.max_tfidf_score = total_doc_scores[doc]
        ################################################################################################
        number_of_relevant_docs_found = len(total_doc_scores)
        # trial and error - retrieve top % of the docs
        if k is None:
            k = number_of_relevant_docs_found

        #  If the query is composed of words from the model try finding the closest doc in the embedding space
        #  Else just sort the docs according the tf-idf
        if Ranker.query_vector.any():
            top_sorted_relevant_docs = Ranker.find_closest_documents(relevant_docs, total_doc_scores)

        else:
            top_sorted_relevant_docs = sorted(total_doc_scores.items(), key=lambda item: item[1], reverse=True)

        return Ranker.retrieve_top_k(list(top_sorted_relevant_docs.keys()), k)

    # -------------------------EMBEDDED RELATED FUNCTIONS-------------------------------------

    @staticmethod
    def find_closest_documents(relevant_docs, total_doc_score):
        """
        The method will sort all the relevant documents based on the total score
        :param relevant_docs:Dict of all the relevant documents
        :param total_doc_score: all the documents score based on TF-IDF
        :return: Sorted list of documents with combination of TF-IDF sim function and Euclidean distance
        """
        max_distance_score = 0
        ranks = {}
        for doc in relevant_docs:
            doc_rank = Ranker.get_total_score(relevant_docs[doc][9], total_doc_score[doc])
            if doc_rank > max_distance_score:
                max_distance_score = doc_rank
            ranks[doc] = doc_rank

        top_sorted_relevant_docs = [
            (doc, (0.95 * (ranks[doc] / max_distance_score) + (0.05 * (total_doc_score[doc] / Ranker.max_tfidf_score))))
            for
            doc, val in ranks.items()]

        return dict(sorted(top_sorted_relevant_docs,
                           key=lambda document: document[1], reverse=True))

    @staticmethod
    def get_total_score(doc_vector, doc_tfidf_score):
        """
        The method will calculate the total score of a document.
        The total score is calculated by using the euclidean distance of the document with query + the combined
        score of the similarity function with the TF-IDF
        :param doc_vector: np.array - the document embedded vector
        :param doc_tfidf_score: float - the score of the similarity functions with the tf-idf
        :return: int. The score of the document with the combined scores
        """
        return 1 * (1 / (spatial.distance.euclidean(doc_vector, Ranker.query_vector)))  # - 0.3 * 1/doc_tfidf_score

    @staticmethod
    def get_cosine(v1, v2):
        """
        Calculate the cosine between two vectors
        :param v1: np array
        :param v2: np array
        :return: float -  Cosine of the two vectors
        """
        return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

    # ----------------------SIMILARITY RELATED FUNCTIONS---------------------------------

    @staticmethod
    def inner_product(relevant_doc):
        """
        The method will calculate the inner product similarity of a document and a query
        :param relevant_doc: List. all the document information
        :return: float. Document inner product score
        """
        inner_product = 0
        for query_term in Ranker.query_terms:
            if query_term in relevant_doc[1]:
                term_index = relevant_doc[1].index(query_term)
                term_weight_doc = relevant_doc[5][term_index]
                term_weight_query = Ranker.query_terms[query_term]
                inner_product += term_weight_doc * term_weight_query
        return inner_product

    @staticmethod
    def BM25_doc_score(doc, corpus_size, k=1.5, b=0.8):
        """
        BM25 Similarity function - return the similarity between the query and document
        :param doc: List - all the document information
        :param corpus_size: int - the corpus size
        :param k: float or int - the bm25 k
        :param b: float - the bm25 b
        :return: float - the bm25's document score
        """
        common_terms = doc[1]
        common_terms_tf = doc[6]
        common_terms_df = doc[7]
        doc_len = doc[3]
        doc_score = 0

        for index, term in enumerate(common_terms):
            term_tf = common_terms_tf[index]
            term_df = common_terms_df[index]
            term_idf = math.log2(corpus_size / term_df)
            numerator = term_tf * (term_tf * (k + 1))
            denominator = term_tf + (k * (1 - b + (b * doc_len / Ranker.avdl)))
            doc_score += (term_idf * (numerator / denominator))

        return doc_score

    @staticmethod
    def cosine_doc_score(doc):
        """
        The method calculates the cosine similarity between two documents based on the tf-idf
        :param doc: List - All the document information
        :return: float - the cosine score
        """
        # numerator -> inner product
        inner_product = 0
        for term_index in range(len(doc[1])):
            term = doc[1][term_index]
            term_weight_doc = doc[5][term_index]
            term_weight_query = Ranker.query_terms[term]
            inner_product += term_weight_doc * term_weight_query

        # denominator left -> term per doc weight squared
        doc_weight = doc[8]
        # denominator right -> term per query weight squared

        cosin_denominator = math.sqrt(doc_weight * Ranker.query_weight)
        cosin_score = inner_product / cosin_denominator

        return cosin_score

    # --------------------K RELATED FUNCTIONS----------------------------

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]

    @staticmethod
    def remove_anomalies(top_ranked, threshold):
        """
        The method will get the threshold  of the data - mean + X*std and remove all the tweets
        with score lower than the threshold
        :param top_ranked: Dict of relevant documents
        :return: Dict - The tweets with higher score than the threshold mapped to their scores
        """
        return dict(filter(lambda doc: doc[1] > threshold, top_ranked.items()))

    @staticmethod
    def get_threshold(scores, n_std=0.0):
        """
         The method will get the threshold K that will cut the results from all the anomalies.
         Anomalies is defined by being with a score below the mean + X*std (default X is 0).
         :param scores: List of documents scores
         :param n_std: X, default is 0
         :return: int - The threshold K
         """
        mean = np.mean(scores)
        std = np.std(scores)
        return mean - (std * n_std)
