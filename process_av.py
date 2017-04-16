import pickle
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer
from tqdm import tqdm
import numpy as np
from multiprocessing import Pool

wl = WordNetLemmatizer()
tokenizer = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=True)

def load_data(fname):
    with open(fname, 'rb') as f:
        return pickle.load(f)

def load_av_data(fname):
    av_data = {}
    with open(fname, 'r') as f:
        start = True
        for line in f:
            if start:
                start = False
                continue
            fields = line.strip().lower().split(',')[1:]
            word, v_mean, v_sd, a_mean, a_sd, d_mean, d_sd = \
                    fields[0], fields[1], fields[2], fields[4], fields[5], fields[7], fields[8]
            av_data[word] = {
                'v_mean': float(v_mean),
                'v_sd': float(v_sd),
                'a_mean': float(a_mean),
                'a_sd': float(a_sd),
                'd_mean': float(d_mean),
                'd_sd': float(d_sd)
            }
    for key, val in av_data.items():
        lemmatized_word = wl.lemmatize(key)
        if key != lemmatized_word and lemmatized_word not in av_data:
            av_data[lemmatized_word] = val
            del av_data[key]
    return av_data

DATA = load_data('./emoji_tweets.pickle')
AV_DATA = load_av_data('./Ratings_Warriner_et_al.csv')

def get_emoji_av(tweets):
    a_sum, v_sum, d_sum = [], [], []
    for tweet in tweets:
        words = tokenizer.tokenize(tweet)
        a_tweet_sum, v_tweet_sum, d_tweet_sum = 0, 0, 0
        total_words = 0
        for word in words:
            lemmatized_word = wl.lemmatize(word)
            if lemmatized_word in AV_DATA:
                av_data = AV_DATA[lemmatized_word]
                total_words += 1
                a_tweet_sum += av_data['a_mean']
                v_tweet_sum += av_data['v_mean']
                d_tweet_sum += av_data['d_mean']
        if total_words > 0:
            a_sum.append(a_tweet_sum / total_words)
            v_sum.append(v_tweet_sum / total_words)
            d_sum.append(d_tweet_sum / total_words)

    return {
        'a_mean': np.mean(a_sum),
        'a_sd': np.std(a_sum),
        'v_mean': np.mean(v_sum),
        'v_sd': np.std(v_sum),
        'd_mean': np.mean(d_sum),
        'd_sd': np.std(d_sum)
    }

def main():
    emoji_av = {}
    pool = Pool(8)
    keys = list(DATA.keys())
    for emoji, av in zip(keys, pool.imap(get_emoji_av, (AV_DATA[key] for key in keys))):
        emoji_av[emoji] = av
    return emoji_av

if __name__ == '__main__':
    emoji_av = main()
