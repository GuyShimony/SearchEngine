from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.misc.wordfinder import word_finder
from nltk.tokenize import word_tokenize
from document import Document
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize import TweetTokenizer
import re
import spacy
import string

class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english') + [",", ";", "`", "/", "~", "\\"]
        self.url_tokenizer = RegexpTokenizer("[\w'+.]+")
      #  self.punctuation_parser = TweetTokenizer(reduce_len=False,strip_handles=
        self.punctuation_dict = dict((ord(char), None) for char in string.punctuation.replace("%", ""))
        self.punctuation_remover = lambda word: word.translate(self.punctuation_dict)
        self.whitespace_tokenizer = WhitespaceTokenizer()
        self.nlp = spacy.load("en_core_web_sm")
        self.sign_dictionary = {
            "#": self.hashtag_parser,
            "@": self.shtrudel_parser,
            "h": self.url_parser
        }
        self.number_dictionary = {
            "thousand": [1000, "K"],
            "millions": [1000000, "M"],
            "billion": [1000000000, "B"],
            "percent": [1, "%"],
            "percentage": [1, "%"]
        }

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        # text_tokens = word_tokenize(text)
        all_text_tokens = self.whitespace_tokenizer.tokenize(text)
        special_text_tokens = self.regex_parser(text)
        text_tokens = [w for w in all_text_tokens if w not in special_text_tokens]
        # text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        text_tokens_without_stopwords = []
        # for word in text_tokens:
        for i in range(len(text_tokens)):
            # if word not in self.stop_words:
            if text_tokens[i] not in self.stop_words:
                word = text_tokens[i]
                word = word.lower()
                word = self.punctuation_remover(word)
                if word.isdigit():
                    if i + 1 >= len(text_tokens):  # Prevent out of bound error
                        self.number_parser(word, "", text_tokens_without_stopwords, text_tokens)
                        break

                    word_after = text_tokens[i + 1]
                    self.punctuation_remover(word_after)
                    self.number_parser(word, word_after, text_tokens_without_stopwords, text_tokens)

                    if i + 2 >= len(text_tokens):  # Prevent out of bound error
                        break
                else:
                    text_tokens_without_stopwords.append(word)
                # # Add each non stop word to the list of words (includes word with @ or #)
                # if 'http' in word or 'https' in word:
                #     self.url_parser(word, text_tokens_without_stopwords)
                # else:
                #     word = self.remove_punctuation(word)
                #     text_tokens_without_stopwords.append(word)

        for special_token in special_text_tokens:
            self.sign_dictionary[special_token[0]](special_token, text_tokens_without_stopwords)

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

    def hashtag_parser(self, hashtaged_word, words_list):

        # split the hashtagged word into english words without the hashtag
        word = hashtaged_word[1:]
        words_list.append(hashtaged_word)

    def shtrudel_parser(self, word, words_list):
        words_list.append(word)

    def url_parser(self, url, words_list):
        parsed_url = self.url_tokenizer.tokenize(url)
        for word in parsed_url:
            if 'www' in word:
                word = word.replace("www.", "")
                words_list.append("www")
                words_list.append(word)
            else:
                words_list.append(word)

    def add_word_to_list(self, words_to_add, words_list):
        for word in words_to_add:
            words_list.append(word)

    def remove_punctuation(self, word):
        return word.translate(dict((ord(char), None) for char in string.punctuation)
)
   #     return self.punctuation_parser.tokenize(word)[0]

    def regex_parser(self, words) -> list:
        return re.findall(
            r'#\w*|@\w*|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))',
            words)

    def number_parser(self, number_word, word_after, words_list, all_words):
        number = int(number_word)
        try:
            word = "{0}{1}".format(number, self.number_dictionary[word_after.lower()][1])
            all_words.remove(word_after)
        except KeyError:
            if len(number_word) < 4:
                word = words_list
            if 4 <= len(number_word) < 6:
                word = str(number / 1000) + "K"
            elif 6 <= len(number_word) < 9:
                word = str(number / 1000000) + "M"
            else:
                word = str(number / 1000000000) + "B"

        words_list.append(word)
