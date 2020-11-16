from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.misc.wordfinder import word_finder
from nltk.tokenize import word_tokenize
from document import Document
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize import TweetTokenizer
import re, spacy, string, pandas as pd


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english') + [",", ";", "`", "/", "~", "\\"]
        self.url_tokenizer = RegexpTokenizer("[\w'+.]+")
        self.punctuation_dict = dict((ord(char), None) for char in string.punctuation.replace("%", ""))
        self.punctuation_remover = lambda word: word.translate(self.punctuation_dict)
        self.whitespace_tokenizer = WhitespaceTokenizer()
        self.nlp = spacy.load("en_core_web_sm")
        self.sign_dictionary = {
            "#": self.hashtag_parser,
            "@": self.shtrudel_parser,
            "h": self.url_parser,
            "w": self.url_parser,
        }
        self.number_dictionary = {
            "thousand": [1000, "K"],
            "million": [1000000, "M"],
            "billion": [1000000000, "B"],
            "percent": [1, "%"],
            "percentage": [1, "%"]
        }
        self.entity_dictionary = {}
        self.capitals_dictionary = {}
        self.capital_df = pd.DataFrame(columns=['Word', 'Lower', 'Upper', 'ToUpper', "Occurrences"]).set_index('Word')

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        # text_tokens = word_tokenize(text)
        # text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        all_text_tokens = self.whitespace_tokenizer.tokenize(text)
        special_text_tokens = self.regex_parser(text)
        capital_letters_tokens = self.capital_tokenizer(text)
        number_tokens = self.numbers_tokenizer(text)
        text_tokens = [w for w in all_text_tokens if w not in special_text_tokens or w not in capital_letters_tokens
                       or w not in number_tokens]

        text_tokens_without_stopwords = []

        # text_tokens_length = len(text_tokens)
        # # for word in text_tokens:
        # for i in range(text_tokens_length):
        #     # TODO need to think of something a lot better than this
        #     if i >= text_tokens_length:
        #         break
        #     if text_tokens[i] not in self.stop_words:
        #         word = text_tokens[i]
        #         word = self.punctuation_remover(word)
        #         #   self.check_for_capital(text_tokens[i], text_tokens)
        #         word = word.lower()
        #         # self.check_for_entity(text_tokens[i], text_tokens)
        #         if word.isdigit():
        #             word_after = text_tokens[i + 1]
        #             self.punctuation_remover(word_after)
        #             was_deleted = self.number_parser(word, word_after, text_tokens_without_stopwords, text_tokens)
        #             if (was_deleted):
        #                 text_tokens_length -= 1
        #         else:
        #             text_tokens_without_stopwords.append(word)
        for word in text_tokens:
            if word not in self.stop_words:
                self.parse_english_words(word, text_tokens_without_stopwords)

        #  handle each 'special word' with its function
        for special_token in special_text_tokens:
            self.sign_dictionary[special_token[0]](special_token, text_tokens_without_stopwords)

        # handle each number word
        for word in number_tokens:
            self.number_parser(word, text_tokens_without_stopwords)
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
        """
        Parse a word containing # to a list of its words split by Upper case letters or '_' + the original
        hashtag word
        """
        words_list += [w.lower() for w in re.findall('[a-z|A-Z][^A-Z|_]*', hashtaged_word)] + \
                      [hashtaged_word[0] + hashtaged_word[1:].lower()]

    def shtrudel_parser(self, word, words_list):
        """
        Parse a word containing @.
        The function will return the original word + the prefix @
        """
        words_list.append(word)

    def url_parser(self, url, words_list):
        """
        Parse a url starting with http(s).
        The function will extract from the url the http(s), the website host name and all following tokens
        that are separated by '\'
        """
        parsed_url = self.url_tokenizer.tokenize(url)
        for word in parsed_url:
            if 'www' in word:
                word = word.replace("www.", "")
                words_list.append("www")
                words_list.append(word)
            else:
                words_list.append(word)

    def add_word_to_list(self, words_to_add, words_list):
        """
        The function will add the word_to_add to the words_list
        """
        for word in words_to_add:
            words_list.append(word)

    def regex_parser(self, words) -> list:
        """
        The function will use a special regular expression to identify all special tokens.
        The special tokens are # @ http.
        Returns a list of all special tokens with there following words
        """
        return re.findall(
            r'#\w*|@\w*|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))',
            words)

    # def number_parser(self, number_word, word_after, words_list, all_words):
    #     """
    #     Parse a string containing a number. The number can be followed by its plural, meaning 123 Thousands can appear
    #     and mean 123000.
    #     The numbers will be saved as 123K or 1.23M (for millions) etc.
    #     """
    #     delete = False
    #     number = int(number_word)
    #     try:
    #         word = "{0}{1}".format(number, self.number_dictionary[word_after.lower()][1])
    #         all_words.remove(word_after)
    #         delete = True
    #     except KeyError:
    #         if len(number_word) < 4:
    #             word = number_word
    #         elif 4 <= len(number_word) < 6:
    #             word = str(number / 1000) + "K"
    #         elif 6 <= len(number_word) < 9:
    #             word = str(number / 1000000) + "M"
    #         else:
    #             word = str(number / 1000000000) + "B"
    #
    #     words_list.append(word)
    #     return delete

    def check_for_entity(self, word_to_check, words_list):
        """
        The function will get a word_to_check and using spacy package will determine if that word
        is a PROPN. If so it will check if it has already occurred in the parse method until that moment and will
        save that propn if so. Else it will remember it for future references.
        """
        for w in self.nlp(word_to_check):
            try:
                if w.pos_ == 'PROPN' and self.entity_dictionary[word_to_check]:
                    words_list.append(word_to_check)
            except KeyError:
                if w.pos_ == 'PROPN':
                    self.entity_dictionary[word_to_check] = 1

    # def check_for_capital(self,word_to_check,words_list):
    # try:
    #     if (word_to_check[0]).isupper() and self.capitals_dictionary[word_to_check.upper()] == 1:
    #         words_list.append(word_to_check.upper())
    #     elif (word_to_check[0]).islower():
    #         words_list.append(word_to_check.lower())
    #         if self.capitals_dictionary[word_to_check.upper()]:
    #             self.capitals_dictionary[word_to_check.upper()] = -1
    # except KeyError:
    #         if self.capitals_dictionary[word_to_check.upper()] and self.capitals_dictionary[word_to_check.upper()] != -1:
    #             self.capitals_dictionary[word_to_check.upper()]=1

    def parse_english_words(self, word_to_check, words_list):
        words_list.append(word_to_check.lower())
        # for w in self.nlp(word_to_check):
        #     if w.pos_ == 'PROPN' and w in self.capital_df.index:
        #         pass


    def capital_tokenizer(self, text):
        return re.findall('[A-Z][^A-Z\s]*', text)

    def numbers_tokenizer(self,text):
        return re.findall("[0-9]+[0-9]*\s+\d+/\d+|[0-9]+[%]*\s[a-zA-Z]*|[+-]?[0-9]+[.][0-9]*[%]*|[.][0-9]+|[0-9]+[%]*"
                          "[^a-zA-z]*",text)


    def number_parser(self, number_word, words_list):
        """
        Parse a string containing a number. The number can be followed by its plural, meaning 123 Thousands can appear
        and mean 123000.
        The numbers will be saved as 123K or 1.23M (for millions) etc.
        """
        number_word, word_after = number_word.split(" ") if len(number_word.split(" ")) == 2 else (number_word, "")
        number_word, word_after = self.punctuation_remover(number_word), self.punctuation_remover(word_after)
        try:
            number = int(number_word)
            word = "{0}{1}".format(number, self.number_dictionary[word_after.lower()][1])
        except KeyError:
            if len(number_word) < 4:
                word = number_word
            elif 4 <= len(number_word) < 6:
                word = str(number / 1000) + "K"
            elif 6 <= len(number_word) < 9:
                word = str(number / 1000000) + "M"
            else:
                word = str(number / 1000000000) + "B"

        except ValueError:
            if "%" in number_word:
                word = number_word

        finally:
            words_list.append(word)
