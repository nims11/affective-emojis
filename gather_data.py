#!/usr/bin/env python3
"""
Gather emoji data from twitter public stream
"""

import time
import logging
from collections import defaultdict, Counter
import pickle
import tweepy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AUTH = tweepy.OAuthHandler(
    'or6Gb6443TBSjAg2oTGWMDPYr',
    't1lwThe4Q8Gz2DBAqbA3FlH78mzb3JZrgLzuJgCIeVuSPPH9aj'
)
AUTH.set_access_token(
    '805254798903799808-FRFBvOtprusOzUZ8QSj8b9wUbMJw5h8',
    'QCtp4wOxJahmCeVjh90LDDGeXwnwpelvDhVPN9QrTzs0j'
)
API = tweepy.API(AUTH)

WAIT_TIME = 60 * 10

def load_emoji_dataset(fname):
    with open(fname) as emoji_file:
        emojis = emoji_file.read().splitlines()
        return [
            bytes(emoji, 'ascii')
            .decode('unicode-escape')
            .encode('utf-16', 'surrogatepass')
            .decode('utf-16')
            for emoji in emojis
        ]

EMOJIS = load_emoji_dataset('./emojis.txt')
TRACK_SIZE = 400

def identify_emoji(string):
    return [emoji for emoji in EMOJIS if emoji in string]

class EmojiStreamListener(tweepy.StreamListener):
    """
    Listen to stream filtering with a set of emoticons
    """
    def __init__(self):
        self.emoji_count = defaultdict(list)
        super(EmojiStreamListener, self).__init__()

    def on_status(self, status):
        if status.lang == 'en':
            for emoji in identify_emoji(status.text):
                self.emoji_count[emoji].append(status.text)

def get_emoji_filter(emoji_filter=None):
    """
    Returns emoji filter for next set of retrievals
    """
    if emoji_filter == None:
        logger.info("Starting from %d" % 0)
        return EMOJIS[:TRACK_SIZE]
    else:
        idx = EMOJIS.index(emoji_filter[-1])
        logger.info("Starting from %d" % idx)
        return (EMOJIS+EMOJIS)[idx+1:idx+TRACK_SIZE+1]

def gather():
    """
    Start the dynamic streaming process
    """
    emoji_stream_listener = EmojiStreamListener()
    stream = tweepy.Stream(API.auth, emoji_stream_listener)
    emoji_filter = get_emoji_filter()

    while True:
        logger.info("beginning stream")
        stream.filter(track=emoji_filter, async=True)
        time.sleep(WAIT_TIME)
        emoji_filter = get_emoji_filter(emoji_filter)
        stream.disconnect()
        logger.info("Tweets for %d emojis!" % len(emoji_stream_listener.emoji_count))
        counts = Counter([len(tweets) for _, tweets in emoji_stream_listener.emoji_count.items()])
        logger.info(['%dx%d' % (x, y) for x, y in sorted(counts.items(), key=lambda x:-x[0])])
        logger.info("dumping data")
        with open('emoji_tweets.pickle', 'wb') as dump_file:
            pickle.dump(emoji_stream_listener.emoji_count, dump_file)

if __name__ == '__main__':
    gather()
