import sys
import time
import math
import argparse
from datetime import datetime

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


class MyStreamListener(StreamListener):

    def __init__(self, limit_tweets=math.inf, limit_time=math.inf):
        """
        Initialize the streaming. You can specify a limit of tweets or
        time (in hours) to cut with the streaming.
        If no conditions were specified, it will continue until you
        press Ctrl + C.

        Parameters
        ----------
        limit_tweets: int, optional
          Limit of tweets to download
        limit_time: int, optinal
          Limit of time in hours to download
        """
        # Limit downloads
        self.limit_tweets = limit_tweets
        self.limit_time = limit_time
        # Create the output file
        date = datetime.now()
        f_name = "streaming_{0}_{1}_{2}.txt".format(date.year, date.month,
                                                    date.day)
        self.file = open(f_name, 'a', encoding='utf-8')
        # Initialize start_time and tweets_counts
        self.start_time = time.time()
        self.amount_tweets = 0

        print("Beginning streaming at: {0}".format(str(datetime.now())))
        print("Output file: {0}".format(f_name))
        print("limit_tweets: {0}. limit_time: {1}".format(self.limit_tweets,
                                                          self.limit_time))

    def on_data(self, data):
        """
        Called when raw data is received from connection.
        """
        self.file.write(data[:-1])
        self.amount_tweets += 1

        # Progress text
        print("\rAmount of tweets: {0}".format(self.amount_tweets), end='')

        # Limit of tweets
        if self.limit_tweets == self.amount_tweets:
            self.file.close()
            return False
        # Limit of time
        if time.time() >= self.start_time + self.limit_time*60*60:
            self.file.close()
            return False

        return True

    def on_error(self, status_code):
        """
        Called when a non-200 status code is returned
        """
        print("Error code: {0}".format(status_code))
        print("For more information on the error code: https://dev.twitter.com/overview/api/response-codes")
        return False


# Parser
parser = argparse.ArgumentParser(description='Twitter Streaming')
parser.add_argument('-c', '--config', dest='config_path',
                    help='Configuration path file')
args = parser.parse_args()


# Configuration dict
config = {}

for line in open(args.config_path, 'r'):
    line = ''.join(line.split())
    if line.startswith('#'):
        continue
    elif line == '':
        continue

    data = [element.replace('\n', '') for element in line.split('=')]
    data = [''.join(element.split()) for element in data]
    config[data[0]] = eval(data[1])


# Check if the keys are valid
valid_keys = ['consumer_key', 'consumer_secret',
              'access_token', 'access_token_secret',
              'keywords']
for key in valid_keys:
    if key not in config.keys():
        print("Error in configuration file. Miss '{0}' key".format(key))
        sys.exit(-1)


# If the limits are not specified, I put them in infinity
if 'limit_tweets' not in config.keys():
    config['limit_tweets'] = math.inf
if 'limit_time' not in config.keys():
    config['limit_time'] = math.inf


# Authentication
auth = OAuthHandler(config['consumer_key'], config['consumer_secret'])
auth.set_access_token(config['access_token'], config['access_token_secret'])
my_stream = MyStreamListener(limit_tweets=config['limit_tweets'],
                             limit_time=config['limit_time'])
stream = Stream(auth, my_stream)

# Filter Twitter Streams to capture data by the keywords
stream.filter(track=config['keywords'])
