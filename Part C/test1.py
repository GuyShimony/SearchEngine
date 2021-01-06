import pandas as pd
import metrics


class MYTEST:

    def __init__(self):
        pass

    def test(self, df_res, df1, i):
        df1 = df1.loc[df1['query'] == i]
        df1 = df1.loc[df1['y_true'] == 1.0]
        tweets = df1.tweet.tolist()
        total = len(tweets)
        our_tweets1 = df_res.tweet_id.tolist()
        our_len = len(our_tweets1)
        sum = 0
        for doc in tweets:
            for ours in our_tweets1:
                if ours == doc:
                    sum += 1

        print("total relevant docs:  ", total)
        print("total relevant docs we retrieved:  ", our_len)
        print("intersection   ", sum)
        # print("RECALL:  ", metrics.recall(df_res, total))
        print("RECALL:  ", sum / total)
        print("PRECISION:  ", sum / our_len)
        print('\n\n\n')


df_res1 = pd.read_csv("results1.csv")
df_res2 = pd.read_csv("results2.csv")
df_res3 = pd.read_csv("results3.csv")
df_res4 = pd.read_csv("results4.csv")
df_res5 = pd.read_csv("results5.csv")
df_res6 = pd.read_csv("results6.csv")
df1 = pd.read_csv("data\\benchmark_lbls_train.csv")

#edna = pd.read_csv("to guy.txt")

t = MYTEST()
t.test(df_res1, df1, 1)
t.test(df_res2, df1, 2)
# t.test(df_res3, df1, 3)
# t.test(df_res4, df1, 4)
t.test(df_res6, df1, 2)
#t.test(edna, df1, 1)
