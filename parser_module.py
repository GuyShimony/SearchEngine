from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.misc.wordfinder import word_finder
from nltk.tokenize import word_tokenize
from document import Document
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize import TweetTokenizer
import re

class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english') + [",", ";", "`", "/", "~", "\\"]
        self.url_tokenizer = RegexpTokenizer("[\w'+.]+")
        self.punctuation_parser = TweetTokenizer()
        self.whitespace_tokenizer = WhitespaceTokenizer()
        self.sign_dictionairy = {
            "#": self.hashtag_parser,
            "@": self.shtrudel_parser(),
        }
    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        #text_tokens = word_tokenize(text)
        text_tokens = self.whitespace_tokenizer.tokenize(text)
        #text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        text_tokens_without_stopwords = []
        for word in text_tokens:
            if word not in self.stop_words:
                word = word.lower()
                # Add each non stop word to the list of words (includes word with @ or #)
                if 'http' in word or 'https' in word:
                    self.url_parser(word, text_tokens_without_stopwords)
                else:
                    word = self.remove_punctuation(word)
                    text_tokens_without_stopwords.append(word)

        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def hashtag_parser(self,hashtaged_word, words_list):

        # split the hashtagged word into english words without the hashtag
        word = hashtaged_word[1:]

    def shtrudel_parser(self):
        pass

    def url_parser(self, url, words_list):
        parsed_url = self.url_tokenizer.tokenize(url)
        for word in parsed_url:
            if 'www' in word:
                word = word.replace("www.","")
                words_list.append("www")
                words_list.append(word)
            else:
                words_list.append(word)

        
    def add_word_to_list(self, words_to_add, words_list):
        for word in words_to_add:
            words_list.append(word)

    def remove_punctuation(self, word):
        return self.punctuation_parser.tokenize(word)[0]