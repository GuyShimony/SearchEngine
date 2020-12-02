import re
import string
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize.regexp import RegexpTokenizer
import spacy
from document import Document


class Parse:

    def __init__(self, stemming=False, lemmatization=False):
        self.stop_words = stopwords.words('english')
        self.url_tokenizer = RegexpTokenizer("[\w'+.]+")
        self.punctuation_dict = dict(
            (ord(char), None) for char in string.punctuation.replace("%", "").replace("@", "").replace("#", ""))
        self.punc = string.punctuation.replace("%", "").replace("@", ""). \
                        replace("#", "").replace("*", "") + "”" + "“" + "•" + "\n"
        self.punctuation_remover = lambda word: (word.lstrip(self.punc)).rstrip(self.punc)
        self.whitespace_tokenizer = WhitespaceTokenizer()
        self.stemmer = SnowballStemmer("english")
        self.nlp = spacy.load('en_core_web_sm', disable=['ner', 'parser', 'tokenizer'])
        self.ascii_words = set(string.printable)
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
            "billions": [1000000000, "B"]
        }
        self.percent_dictionary = {
            "percent": ["%"],
            "percents": ["%"],
            "percentage": ["%"],
            "percentages": ["%"]
        }
        self.entity_dictionary = {}
        self.tweet_id = None
        self.stem = stemming
        self.lemma = lemmatization
        self.coronavirus_dictionary = {
            #  Custom coronavirus rule -> Switch any coronavirus term form to 'coronavirus'
            #  Used to better IR coronavirus related docs
            "covid": "coronavirus",
            "COVID": "coronavirus",
            "covid-19": "coronavirus",
            "COVID-19": "coronavirus",
            "Covid-19": "coronavirus",
            "covid19": "coronavirus",
            "covid_19": "coronavirus",
            "coronavirus": "coronavirus"
        }
        self.USA_dictionary = {
            #  Custom coronavirus rule -> Switch any usa / america term form to 'USA'
            #  Used to better IR usa related docs
            "u.s": "USA",
            "u.s.": "USA",
            "usa": "USA",
            "u.s.a": "USA",
            "america": "USA"
        }
        self.excluded_data = ["t.co", "https", "http", "html", "t"]

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text: The text to parse. Representation in string
        :param stem: To use or not to use stemming on the text
        :return:
        """

        # added rules: 1. coronavirus, 2. usa, 3. dates, 4. number+identifier, 5. curses into *, 6. emojis,
        # 7. word with / (hello/world -> hello world), 8. remove bold words, 9. lemmatization 10. tweet id removal

        if text is None or text == '[]':
            return {}  # Return an empty dict
        try:
            # Preprocessing - remove all 'RT' (retweet mention doesnt aid in the retrieval process)
            # only if RT is appeared alone .. not as part of a word
            text = text.replace(" RT ", "")
            text = text[0:3].replace("RT ", "") + text[3:]  # Remove RT at the beginning of the tweet
            text = text.replace("[", "").replace("]", "")
        except AttributeError:
            return {}
        # Preprocessing - Apply the curse rule first to replace each curse word with the word CENSORED
        text = self.curse_parser(text)
        for w in self.tweet_id_tokezizer(text):
            text = text.replace(w, "")  # Remove tweet id from text


        # Apply all the tokenizing rules on the text #
        special_text_tokens = self.special_cases_tokenizer(text)
        for word in special_text_tokens:
            text = text.replace(word, "")

        text = self.covid_normelizer(text)
        text = self.usa_normelizer(text)

        text_tokens_without_stopwords = {}

        all_text_tokens = self.whitespace_tokenizer.tokenize(text)

        # First step - add each word (that was separated by white space) to the dictionary as a token
        for word in all_text_tokens:
            try:
                if re.search("[…]+", word) or len(word) == 1:  # 3 twitter type dots (end of tweet) or single letters
                    continue

                # Remove all non ascii chars and punctuations
                word = ''.join(filter(lambda w: w in self.ascii_words and w not in self.punc.replace("/", ""), word))

                if re.search("[…]+", word) or len(
                        word) == 1 or not word:  # 3 twitter type dots (end of tweet) or single letters
                    continue

                elif word.lower() not in self.stop_words and word[0] != "#" and word[0] != "@" and word[:2] != "ht" \
                        and word[:2] != "ww":

                    if "/" in word:  # Handle terms like 'corona/people' -> add 'corona' & 'people'
                        all_text_tokens += [w for w in word.split("/")]
                        continue

                    if word.lower() in self.coronavirus_dictionary:
                        word = self.coronavirus_dictionary[word.lower()]
                        text_tokens_without_stopwords[word] += 1

                    elif word.lower() in self.USA_dictionary:
                        word = self.USA_dictionary[word.lower()]
                        text_tokens_without_stopwords[word] += 1

                    else:
                        text_tokens_without_stopwords[word] = text_tokens_without_stopwords[word] + 1
            except KeyError:
                text_tokens_without_stopwords[word] = 1

        # Second step - Apply all the tokenizing rules on the text #
        number_tokens, irregulars = self.numbers_tokenizer(text)
        date_tokens = self.date_tokenizer(text)

        # Third step - delete all the words that were processed in the rules.
        # For example '123 Thousand' was turned to '123K' -> Need to delete  '123', 'Thousand'
        for irregular in irregulars:
            try:
                irregular = irregular.lower()
                if text_tokens_without_stopwords[irregular] > 1:
                    text_tokens_without_stopwords[irregular] -= 1
                else:
                    text_tokens_without_stopwords.pop(irregular)
            except KeyError:
                pass

        # Fourth step - Apply all the parsing rules
        # For example - turn '123 Thousand' to '123K'
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
                word = word.strip("\n")
                if "\n" in word:
                    rule_generated_tokens += [w for w in word.split("\n")]
                    continue

                if not word or len(word) == 1:
                    # Ignore single letters words
                    continue

                if word in self.coronavirus_dictionary:
                    text_tokens_without_stopwords[self.coronavirus_dictionary[word.lower()]] += 1
                elif word in self.USA_dictionary:
                    text_tokens_without_stopwords[self.USA_dictionary[word.lower()]] += 1
                else:
                    text_tokens_without_stopwords[word] = text_tokens_without_stopwords[word.lower()] + 1
            except KeyError:
                if word in self.coronavirus_dictionary:
                    text_tokens_without_stopwords[self.coronavirus_dictionary[word.lower()]] = 1
                elif word in self.USA_dictionary:
                    text_tokens_without_stopwords[self.USA_dictionary[word.lower()]] = 1
                else:
                    text_tokens_without_stopwords[word] = 1

        for date in date_tokens:
            try:
                if len(date) == 1:  # Ignore single letters words
                    continue
                text_tokens_without_stopwords[date.lower()] = text_tokens_without_stopwords[date.lower()] + 1
            except KeyError:
                text_tokens_without_stopwords[date.lower()] = 1

        # Six step - Apply Entity Recognition on the text and add the entities
        # If the entity 'Donald Trump" was recognize as an entity we won't delete the existing tokens:
        # 'donlad', 'trump' from the dictionary. The reason is for queries like "Mr Trump".
        # Queries like this will not be matched if only 'donald trump' will be in the doc
        # for entity in self.entity_recognizer(text):
        entities = self.entity(text)
        if entities:
            for entity in self.entity(text):
                try:
                    text_tokens_without_stopwords[entity] = text_tokens_without_stopwords[entity] + 1
                except KeyError:
                    text_tokens_without_stopwords[entity] = 1
        # stem or lemmatization (not both, stem gets priority)
        if self.stem:
            text_tokens_without_stopwords_stemmed = {}
            for word in text_tokens_without_stopwords:
                word = self.stemmer.stem(word)
                if word not in text_tokens_without_stopwords_stemmed:
                    text_tokens_without_stopwords_stemmed[word] = 1
                else:
                    text_tokens_without_stopwords_stemmed[word] += 1
            return text_tokens_without_stopwords_stemmed
        elif self.lemma:
            lemmatized_word = ''
            text_tokens_without_stopwords_lemma = {}
            for word in text_tokens_without_stopwords:
                word_sentence = self.nlp(word)
                for w in word_sentence:
                    lemmatized_word = " ".join([w.lemma_ for w in word_sentence])
                if lemmatized_word not in text_tokens_without_stopwords_lemma:
                    text_tokens_without_stopwords_lemma[lemmatized_word] = 1
                else:
                    text_tokens_without_stopwords_lemma[lemmatized_word] += 1
            return text_tokens_without_stopwords_lemma

        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list representing the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        self.tweet_id = tweet_id
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        if type(url) is float:  # Handle the bug that some urls are read as 'nan' float
            url = "{}"
        url = url.replace("{", "").replace("}", "").replace('"', "").replace("[", "").replace("]", "")
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]

        url = url.replace("{", "").replace("}", "").replace('"', "").replace("[", "").replace("]", "")
        if url:
            urls_index = [m.start() for m in re.finditer('http', url)]  # Find all start index of the http word
            urls = [url[:i - 1] if i - 1 > 0 else url[:i] for i in urls_index] + [url[urls_index[-1]:]]  # Match all url
            url = "".join(w + " " for w in urls)  # Join on all urls with spaces as a separator
            url_dict = self.parse_sentence(url)
        else:
            url_dict = {}

        full_text_dict = self.parse_sentence(full_text)

        # Merge all dict objects to one with dictionaries unpacking
        term_dict = {**full_text_dict, **url_dict}

        # doc_length = len(term_dict)  # after text operations.
        doc_length = sum(term_dict.values())  # after text operations.
        # To avoid tweets that do not follow any parsing rule. For example the full text is 'same' (stop word)
        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)

        return document

    def replace_short_extended_url(self, full_text, url):
        short_urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|'
                                r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|'
                                r'https?:\/\/(?:www\.|(?!www))',
                                full_text)
        urls = url.replace("[", "").replace("]", "").split(";")
        for short, extended in zip(short_urls, urls):
            full_text = full_text.replace(short, extended)

        return full_text
    
    def covid_tokenizer(self, text):
        return re.findall("[^@#][Cc][Oo][Vv][Ii][Dd][-]*[19]*|"
                          "[^@#][Cc][Oo][Rr][Oo][Nn][Aa][Vv][Ii][Rr][Uu][Ss]",text)

    def covid_normelizer(self, text):
        for word in self.covid_tokenizer(text):
            try:
                word = word.strip(" ")
                text = text.replace(word, self.coronavirus_dictionary[word.lower()])
            except:
                pass

        return text

    def usa_tokenizer(self, text):
        return re.findall("[^@#][Uu][.]*[Ss][.]*[Aa]|[^@#a-tv-zA-TV-Z][U][.]*[S][.]*[^a-tv-zA-TV-Z]", text)

    def usa_normelizer(self, text):
        for word in self.usa_tokenizer(text):
            try:
                word = word.strip(" ")
                text = text.replace(word, self.USA_dictionary[word.lower()])
            except:
                pass

        return text

    def tweet_id_tokezizer(self, text):
        """
        The function looks for tweet id in the text (20 length numbers) and return them
        as tokens
        """
        return re.findall("\d{20}|\d{19}", text)

        ######## RULE BASED TOKENIZER FUNCTIONS #############

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
            r'#\w+-\d+|#\w+|@\w+-\d+|@\w*|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|'
            r'https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))',
            text)

    def numbers_tokenizer(self, text):
        """
        Used to find all numbers tokens with special words that come after
        like 'Thousand', 'Percentage' and some more words that can be found in the number rule parsing
        in the exercise
        """
        number_and_words = "[^a-zA-Z\s][0-9]+\s[tT]housand[s]*\s*[a-zA-Z]*|" \
                           "[^a-zA-Z\s][0-9]+\s[mM]illion[s]*\s*[a-zA-Z]*|" \
                           "[^a-zA-Z\s][0-9]+\s[bB]illion[s]*\s*[a-zA-Z]*|" \
                           "[^a-zA-Z\s][0-9]+\s[pP]ercent[age]*[s]*\s*[a-zA-Z]*|"
        return re.findall("\d+%|[0-9]+[0-9]*\s+\d+/\d+|[+-]?[0-9]+[.][0-9]*[%]*|[.][0-9]+|"
                          "[^#-@\sa-zA-Z][^#-@\sa-zA-Z][0-9]+|" + number_and_words + "[0-9]+[\s]+[a-zA-Z]+|[0-9]+"
                          , text), \
               [words for segment in
                re.findall("[0-9]+[0-9]*\s+\d+/\d+|" + number_and_words + "[0-9]+[\s]+[a-zA-Z]+|[0-9]+", text) for
                words in segment.split()]

    ######## RULE BASED PARSER FUNCTIONS #############

    def hashtag_parser(self, hashtag_word, words_list):
        """
        Parse a word containing # to a list of its words split by Upper case letters or '_' + the original
        hashtag word
        """
        try:
            if len(hashtag_word) == 2 or hashtag_word[1].isupper() and hashtag_word[2].isupper():
                words_list.append(hashtag_word)
                words_list.append(hashtag_word[1:])

            else:
                words_list += [w.lower() for w in re.findall('[a-z|A-Z][^A-Z|_]*', hashtag_word)] + \
                              [hashtag_word[0] + hashtag_word[1:].lower()]
        except IndexError as i:
            pass

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
        if "t.co" in url:
            return

        parsed_url = self.url_tokenizer.tokenize(url)
        for word in parsed_url:
            if word in self.excluded_data:
                continue

            elif 'www' in word: # Take the domain without the www.
                words_list.append(word[4:])

            else:
                words_list.append(word)

    def number_parser(self, number_word, words_list):
        """
        Parse a string containing a number. The number can be followed by its plural, meaning 123 Thousands can appear
        and mean 123000.
        The numbers will be saved as 123K or 1.23M (for millions) etc.
        """
        sign = ''
        word_after = ""
        word_last = ""
        if len(number_word.split(" ")) == 3:
            number_word, word_after, word_last = number_word.split(" ")
        elif len(number_word.split(" ")) == 2:
            number_word, word_after = number_word.split(" ")

        if word_last:
            number_word, word_after, word_last = self.punctuation_remover(number_word), self.punctuation_remover(
                word_after), \
                                                 self.punctuation_remover(word_last)
        elif word_after and not word_last:
            number_word, word_after = self.punctuation_remover(number_word), self.punctuation_remover(word_after)
        else:
            number_word = self.punctuation_remover(number_word)

        try:
            #  Handle floating points and fractions numbers like '2 1/3'
            number = float(number_word) if "." in number_word else int(number_word)
            if "/" in word_after:
                word = number_word + " " + word_after
            else:
                if word_after in self.number_dictionary:
                    number = number * int(self.number_dictionary[word_after.lower()][0])
                    number_word = str(number)  # number with large value
                else:
                    sign = self.percent_dictionary[word_after.lower()][0]
                    number_word = str(number)
                if len(number_word) < 4 or "." in number_word:
                    word = number_word
                elif 4 <= len(number_word) <= 6:
                    word = number_word[:-3] + "K"
                elif 6 <= len(number_word) <= 9:
                    word = number_word[:-6] + "M"
                else:
                    word = number_word[:-9] + "B"
                if sign: # For the percentage
                    word += sign

                if word_last:
                    words_list.append(word)
                    if word_last.lower() in self.percent_dictionary:
                        word_last = self.percent_dictionary[word_last.lower()][0]
                        word = word + word_last
                    else:
                        word = str(word) + " " + word_last
                        words_list.append(word_last)

        except KeyError:
            if len(number_word) < 4 or "." in number_word:
                word = number_word
            elif 4 <= len(number_word) <= 6:
                word = str(number / 1000) + "K"
            elif 7 <= len(number_word) <= 9:
                word = str(number / 1000000) + "M"
            else:
                word = str(number / 1000000000) + "B"
            if word_after:
                words_list.append(word)
                word = word + " " + word_after
                words_list.append(word_after)

        except ValueError:
            word = number_word

        finally:
            words_list.append(word)

    def curse_parser(self, text):
        curse_tokens = self.curse_tokenizer(text)
        for word in curse_tokens:
            text.replace(word, "*CENSORED*")

        return text

    def entity(self, text):
        """
        According to the exercise: "An entity is a pair (or more) of following terms starting with capital letters.
        That occurred in two documents or more".
        The function will find this pattern and insert it to the entity dictionary.
        If the entity has already been in another document it will be matched and returned.

        The algorithm - Use Regular expression to find all Capitalized words. Iterate each pair and search the delta
        of the words in the original text, meaning if they were close words separated by a delimiter.
        If the delta is -1 it will be considered a match.
        """
        words_list = []
        entities = re.findall("[A-Z]+[a-z]*[-’A-za-z]*[19]*", text)
        if len(entities) > 1:  # Check only lists of more than 1 capital word
            for i in range(1, len(entities)):
                if entities[i].lower() in self.stop_words or entities[i - 1] in self.stop_words:  # Ignore stop word
                    continue

                if len(entities[i]) == 1 and len(entities[i - 1]) == 1:
                    continue

                if text.find(entities[i - 1]) + len(entities[i - 1]) - text.find(entities[i]) == -1:
                    separator = text.find(entities[i]) - 1
                    word = f"{entities[i - 1]}{text[separator]}{entities[i]}"
                    try:
                        #  Search the entity dictionary to see if the term has already been matched by another doc
                        if self.entity_dictionary[word] and \
                                self.entity_dictionary[word] != self.tweet_id:
                            words_list.append(word)

                    except KeyError:
                        self.entity_dictionary[word] = self.tweet_id

        return words_list
