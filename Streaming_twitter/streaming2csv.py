"""
Read the raw text file generate by the streaming.py script and create
a reduce csv file.
"""

import sys
import csv
import json
import argparse

import pandas as pd


def create_csv(streaming_path, csv_path):
    """
    Create the base csv

    Parameter
    streaming_path: str
      Path to the output file of streaming.py script
    csv_path: str
      Path to the putpur csv file
    """
    csv_head = ('timestamp_ms,id,user,following,follower,location,time_zone,'
                'tweet_id,is_retweet,mentions_id,'
                'mentions_username,hashtags,tweet\n')
    line_template = """{0},{1},"{2}",{3},{4},"{5}","{6}",{7},{8},"{9}","{10}","{11}","{12}"\n"""

    # Open output csv file
    csv = open(csv_path, 'a')
    # Write the header
    csv.write(csv_head)

    with open(streaming_path, 'r') as file:
        for cnt, line in enumerate(file):
            tweet = json.loads(line)

            if 'user' not in tweet.keys():
                continue

            mentions_id = [user['id'] for user in
                           tweet['entities']['user_mentions']]
            mentions_username = [user['screen_name'] for user in
                                 tweet['entities']['user_mentions']]
            hashtags = [hashtag['text'] for hashtag in
                        tweet['entities']['hashtags']]
            text = tweet['text'].replace('"', "'")

            if 'retweeted_status' in tweet.keys():
                tweet_id = tweet['retweeted_status']['id']
                retweet = True
            else:
                tweet_id = tweet['id']
                retweet = False

            if tweet['user']['location'] is not None:
                location = tweet['user']['location'].replace(',', ' ')
            else:
                location = ''

            if tweet['user']['time_zone'] is not None:
                time_zone = tweet['user']['time_zone'].replace(',', ' ')
            else:
                time_zone = ''

            text = line_template.format(tweet['timestamp_ms'],
                                        tweet['user']['id'],
                                        tweet['user']['screen_name'],
                                        tweet['user']['friends_count'],
                                        tweet['user']['followers_count'],
                                        location, time_zone, tweet_id, retweet,
                                        mentions_id, mentions_username,
                                        hashtags, text)

            csv.write(text)

    csv.close()


# Parser
parser = argparse.ArgumentParser(description='Streaming to CSV')
parser.add_argument('-f', '--file', dest='file_path',
                    help='Text raw file path file')
args = parser.parse_args()


# Creating the csv
print("Creating the csv file: ...", end='')
sys.stdout.flush()
csv_path = args.file_path.replace('txt', 'csv')
create_csv(args.file_path, csv_path)
print("\rCreating the csv file: Done")
sys.stdout.flush()

# Read the created csv
df = pd.read_csv(csv_path)

unique_tweet_id = df.tweet_id.unique()
print("Total tweets: {0}\nUniques tweets: {1}".format(df.shape[0],
                                                      len(unique_tweet_id)))
sys.stdout.flush()

# Counting the tweets
print("Counting the tweets: ...", end='')
sys.stdout.flush()
count_uniques = df.tweet_id.value_counts()
df['tweets_count'] = df.tweet_id.apply(lambda x: count_uniques[x])

print("\rCounting the tweets: Done")
sys.stdout.flush()

# Save the df
print("Saving the csv: ...", end='')
df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
print("Saving the csv: Done", end='')
