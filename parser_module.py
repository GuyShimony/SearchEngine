from nltk.corpus import stopwords
from nltk.corpus import words
from nltk.misc.wordfinder import word_finder
from nltk.tokenize import word_tokenize
from document import Document
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize import TweetTokenizer
import re, spacy, string, pandas as pd
from nltk.stem.snowball import SnowballStemmer


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english') + [",", ";", "`", "/", "~", "\\"]
        self.url_tokenizer = RegexpTokenizer("[\w'+.]+")
        self.punctuation_dict = dict(
            (ord(char), None) for char in string.punctuation.replace("%", "").replace("@", "").replace("#", ""))
        self.punc = string.punctuation.replace("%", "").replace("@", "").replace("#", "").replace("*", "")
        # self.punctuation_remover = lambda word: word.translate(self.punctuation_dict)
        self.punctuation_remover = lambda word: (word.lstrip(self.punc)).rstrip(self.punc)
        self.whitespace_tokenizer = WhitespaceTokenizer()
        self.stemmer = SnowballStemmer("english")
        self.nlp = spacy.load("en_core_web_sm")  # Used for entity recognition
        self.sign_dictionary = {
            "#": self.hashtag_parser,
            "@": self.shtrudel_parser,
            "h": self.url_parser,
            "w": self.url_parser,
        }
        self.number_dictionary = {
            "thousand": [1000, "K"],
            "thousands": [1000, "K"],
            "million": [1000000, "M"],
            "millions": [1000000, "M"],
            "billion": [1000000000, "B"],
            "billions": [1000000000, "B"],
            "percent": [1, "%"],
            "percents": [1, "%"],
            "percentage": [1, "%"],
            "percentages": [1, "%"]
        }
        self.entity_dictionary = {}
        #self. = {}
        # self.capital_df = pd.DataFrame(columns=['Word', 'Lower', 'Upper', 'ToUpper', "Occurrences"]).set_index('Word')
        self.tweet_id = None

    def parse_sentence(self, text, stem=False):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        # Preprocessing - Apply the curse rule first to replace each curse word with the word CENSORED
        text = self.curse_parser(text)
        text_tokens_without_stopwords = {}

        # text_tokens = word_tokenize(text)
        # text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        all_text_tokens = self.whitespace_tokenizer.tokenize(text)
        # First step - add each word (that was separated by white space) to the dictionary as a token
        for word in all_text_tokens:
            try:
                if word not in self.stop_words and word[0] not in self.sign_dictionary.keys():
                    # TODO Talk to guy if needed - sequence of emoji are considered one
                    word = self.punctuation_remover(word).lower()
                    text_tokens_without_stopwords[word] = text_tokens_without_stopwords[word] + 1

            except KeyError:
                text_tokens_without_stopwords[word] = 1

        # Second step - apply all the tokenizing rules on the text
        special_text_tokens = self.special_cases_tokenizer(text)
        number_tokens, irregulars = self.numbers_tokenizer(text)
        date_tokens = self.date_tokenizer(text)

        # Third step - delete all the words that were processed in the rules.
        # For example '123 Thousand' was turned to '123K' -> Need to delete  '123', 'Thousand'

        for irregular in irregulars:
            try:
                irregular = irregular.lower()
                text_tokens_without_stopwords.pop(irregular)
            except KeyError:
                pass

        # Fourth step - Apply all the parsing rules/
        # For example - turn '123 Thousand' to
        # handle all regular words

        rule_generated_tokens = []
        #  handle each 'special word' with its function
        for special_token in special_text_tokens:
            self.sign_dictionary[special_token[0]](special_token, rule_generated_tokens)

        # handle each number word
        for word in number_tokens:
            self.number_parser(word, rule_generated_tokens)

        # Fifth step - add all the newly generated tokens to the dict

        for word in rule_generated_tokens:
            try:
                text_tokens_without_stopwords[word] = text_tokens_without_stopwords[word] + 1
            except KeyError:
                text_tokens_without_stopwords[word] = 1

        for date in date_tokens:
            try:
                text_tokens_without_stopwords[date.lower()] = text_tokens_without_stopwords[date.lower()] + 1
            except KeyError:
                text_tokens_without_stopwords[date.lower()] = 1

        # Six step - Apply Entity Recognition on the text and add the entities
        # If the entity 'Donald Trump" was recognize as an entity we won't delete the existing tokens:
        # 'donlad', 'trump' from the dictionary. The reason is for queries like "Mr Trump".
        # Queries like this will not be matched if only 'donald trump' will be in the doc
        for entity in self.entity_recognizer(text):
            try:
                text_tokens_without_stopwords[entity] = text_tokens_without_stopwords[entity] + 1
            except KeyError:
                text_tokens_without_stopwords[entity] = 1

        if (stem):
            text_tokens_without_stopwords_stemmed = []
            for word in text_tokens_without_stopwords:
                word = self.stemmer.stem(word)
                text_tokens_without_stopwords_stemmed.append(word)
            return text_tokens_without_stopwords_stemmed

        return text_tokens_without_stopwords

        # text_tokens = []
        # self.check_for_entity(text, text_tokens)
        # for w in all_text_tokens:
        #     word = self.punctuation_remover(w)
        #     if word not in special_text_tokens and word not in number_tokens and word not in irregular_numbers \
        #             and word not in irregular_dates:
        #         text_tokens.append(word)
        #
        # text_tokens_without_stopwords = []

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

        # Fourth step - Apply all the parsing rules/
        # For example - turn '123 Thousand' to
        # handle all regular words
        # for word in text_tokens:
        #     if word not in self.stop_words and word:
        #         # word is curse word
        #         if "**" in word:
        #             self.curse_words(text_tokens_without_stopwords)
        #         else:
        #             self.parse_english_words(word, text_tokens_without_stopwords)


    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        self.tweet_id = tweet_id
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]

        # tokenized_text = self.parse_sentence(full_text)
        term_dict = self.parse_sentence(full_text)

        # doc_length = len(tokenized_text)  # after text operations.
        doc_length = len(term_dict.keys())  # after text operations.

        # for term in tokenized_text:
        #     if term not in term_dict.keys():
        #         term_dict[term] = 1
        #     else:
        #         term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    ######## RULE BASED TOKENIZER FUNCTION #############

    def curse_tokenizer(self, text):
        """
        Custom rule 1 - Identify curser in the text with the format of F**k.
        Return each curse as a token
        """
        return re.findall("[a-zA-Z][**]+[a-zA-z]*", text)

    def date_tokenizer(self, text):
        """
        Custom rule 2 - Add each date in the format of Jun 2008 or Jun 08
        As a token.
        Reason: Docs with the phrase Jun 2008 will be returned immediately without the need
        to merge Docs that contain 'Jun' and Docs that contains '2008'
        """
        short_month = "[jJ][aA][nN]\s[0-9][0-9]+|[fF][eE][bB]\s[0-9][0-9]+" \
                      "|[mM][aA][rR]\s[0-9][0-9]+|[aA][pP][rR]\s[0-9][0-9]+|" \
                      "[mM][aA][yY]\s[0-9][0-9]+|[jJ][uU][nN]\s[0-9][0-9]+" \
                      "|[jJ][uU][lL]\s[0-9][0-9]+|[aA][uU][gG]\s[0-9][0-9]+" \
                      "|[sS][eE][pP]\s[0-9][0-9]+|[oO][cC][tT]\s[0-9][0-9]+" \
                      "|[nN][oO][vV]\s[0-9][0-9]+|[dD][eE][cC]\s[0-9][0-9]+"

        full_month = "[jJ]anuary\s[0-9][0-9]+|[fF]ebruary\s[0-9][0-9]+|" \
                     "[mM]arch\s[0-9][0-9]+|[aA]pril\s[0-9][0-9]+|" \
                     "[mM]ay\s[0-9][0-9]+|[jJ]une\s[0-9][0-9]+|" \
                     "[jJ]uly\s[0-9][0-9]+|[aA]ugust\s[0-9][0-9]+|" \
                     "[sS]eptember\s[0-9][0-9]+|[oO]ctober\s[0-9][0-9]+|" \
                     "[nN]ovember\s[0-9][0-9]+|[dD]ecember\s[0-9][0-9]+"
        month_year_regex = short_month + "|" + full_month
        return re.findall(month_year_regex, text)

    def special_cases_tokenizer(self, text) -> list:
        """
        The function will use a special regular expression to identify all special tokens.
        The special tokens are # @ http.
        Returns a list of all special tokens with there following words
        """
        return re.findall(
            r'#\w*|@\w*|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|'
            r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))',
            text)

    def entity_recognizer(self, text):
        """
        The function will get a word_to_check and using spacy package will determine if that word
        is a Entity. If so it will check if it has already occurred in the parse method until that moment and will
        save that Entity if so. Else it will remember it for future references.
        """
        words_list = []
        for word_to_check in self.nlp(text).ents:
            try:
                if self.entity_dictionary[word_to_check] and \
                        self.entity_dictionary[word_to_check] != self.tweet_id:
                    words_list.append(word_to_check)
            except KeyError:
                self.entity_dictionary[word_to_check] = self.tweet_id

        return words_list

    def capital_tokenizer(self, text):
        return re.findall('[A-Z][^A-Z\s]*', text)

    def numbers_tokenizer(self, text):
        number_and_words = "[^a-zA-Z\s][0-9]+\s[tT]housand[s]*|" \
                           "[^a-zA-Z\s][0-9]+\s[mM]illion[s]*|" \
                           "[^a-zA-Z\s][0-9]+\s[bB]illion[s]*|" \
                           "[^a-zA-Z\s][0-9]+\s[pP]ercent[age]*[s]*"

        return re.findall("\d+%|[0-9]+[0-9]*\s+\d+/\d+|[+-]?[0-9]+[.][0-9]*[%]*|[.][0-9]+|"
                          "[^#-@\sa-zA-Z][^#-@\sa-zA-Z][0-9]+|" + number_and_words
                          , text), \
               [words for segment in re.findall("[0-9]+[0-9]*\s+\d+/\d+|" + number_and_words, text) for
                words in segment.split()]

   ######## RULE BASED PARSER FUNCTION #############

    def hashtag_parser(self, hashtaged_word, words_list):
        """
        Parse a word containing # to a list of its words split by Upper case letters or '_' + the original
        hashtag word
        """
        if hashtaged_word[1].isupper():
            words_list.append(hashtaged_word)
            words_list.append(hashtaged_word[1:])

        else:
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

    def number_parser(self, number_word, words_list):
        """
        Parse a string containing a number. The number can be followed by its plural, meaning 123 Thousands can appear
        and mean 123000.
        The numbers will be saved as 123K or 1.23M (for millions) etc.
        """
        number_word, word_after = number_word.split(" ") if len(number_word.split(" ")) == 2 else (number_word, "")
        number_word, word_after = (self.punctuation_remover(number_word), self.punctuation_remover(word_after)) \
            if word_after else (self.punctuation_remover(number_word), "")
        try:
            number = float(number_word) if "." in number_word else int(number_word)
            if "/" in word_after:
                word = number_word + " " + word_after
            else:
                number = number * int(self.number_dictionary[word_after.lower()][0])
                number_word = str(number)
                # word = "{0}{1}".format(number, self.number_dictionary[word_after.lower()][1])
                if len(number_word) < 4 or "." in number_word:
                    word = number_word
                elif 4 <= len(number_word) <= 6:
                    word = number_word[:-3] + "K"
                elif 6 <= len(number_word) <= 9:
                    word = number_word[:-6] + "M"
                else:
                    word = number_word[:-9] + "B"

        except KeyError:
            if len(number_word) < 4 or "." in number_word:
                word = number_word
            elif 4 <= len(number_word) <= 6:
                word = str(number / 1000) + "K"
            elif 7 <= len(number_word) <= 9:
                word = str(number / 1000000) + "M"
            else:
                word = str(number / 1000000000) + "B"

        except ValueError:
            if "%" in number_word:
                word = number_word

        finally:
            words_list.append(word)

    def curse_parser(self, text):
        curse_tokens = self.curse_tokenizer(text)
        for word in curse_tokens:
            text.replace(word, "*CENSORED*")

        return text
        #words_list.append("*CENSORED*")

    def entity_recognizer(self, text):
        """
        The function will get a word_to_check and using spacy package will determine if that word
        is a Entity. If so it will check if it has already occurred in the parse method until that moment and will
        save that Entity if so. Else it will remember it for future references.
        """
        words_list = []
        for word_to_check in self.nlp(text).ents:
            try:
                if self.entity_dictionary[word_to_check] and \
                        self.entity_dictionary[word_to_check] != self.tweet_id:
                    words_list.append(word_to_check)
            except KeyError:
                self.entity_dictionary[word_to_check] = self.tweet_id

        return words_list
