import requests
import json
import base64
import os
import sys

class TweetApiRetriever(object):
  def __init__(self, consumerToken, consumerSecret):
    # set the consumer token and secret
    self.ID = set()
    self.consumerToken = consumerToken
    self.consumerSecret = consumerSecret

    self.encodedTokenSecret = None
    self.accessToken = None

    # set the twitter file info
    self.tweetFileCount = 18
    self.maxTweetFileSize = 10485760 # 10 Mb

    # combined tweet file sizes are 2 GB
    self.totalTweetFileSize = 21474836480

    # ids of the tweets
    #self.visitedTweetIds = set()

  # encode the consumer token and secret in base64 format
  def createEncodedTokenSecret(self):
    consumerTokenSecret = '{}:{}'.format(self.consumerToken, self.consumerSecret)
    encodedConsumerTokenSecret = base64.b64encode(bytes(consumerTokenSecret, 'utf-8'))

    self.encodedTokenSecret = encodedConsumerTokenSecret.decode('utf-8')

  def getEncodedTokenSecret(self):
    return self.encodedTokenSecret

  # get an access token from the base64 encoded consumer token secret
  def createAccessToken(self):
    headers = {
      'Authorization': 'Basic {}'.format(self.getEncodedTokenSecret())
    }

    accessTokenResponse = requests.post(
      'https://api.twitter.com/oauth2/token?grant_type=client_credentials',
      headers=headers
    )

    self.accessToken = accessTokenResponse.json()['access_token']

  def getAccessToken(self):
    return self.accessToken

  ''' Stream tweets from the Twitter API and write them to a file '''

  def streamTweets(self):
    headers = {
      'Authorization': 'Bearer {}'.format(self.getAccessToken())
    }

    try:
      getResponse = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers
      )
      if 'data' in getResponse.text:
        ids = [i['id'] for i in (json.loads(getResponse.text))['data']]
        # delete rules if any exist
        deleteResponse = requests.post(
          "https://api.twitter.com/2/tweets/search/stream/rules",
          headers=headers,
          json={
            "delete": {
              "ids": ids
            }
          }
        )

      postResponse = requests.post(
        'https://api.twitter.com/2/tweets/search/stream/rules',
        headers=headers,
        json={
          "add": [
            { "value": "marvel has:links" },
            { "value": "marvel" },
            { "value": "(doctor strange) has:links" },
            { "value": "(doctor strange)" },
            { "value": "(iron man) has:links" },
            { "value": "(iron man)" },
            { "value": "thor has:links" },
            { "value": "thor" },
            { "value": "spiderman has:links" },
            { "value": "spiderman" },
            { "value": "(captain america) has:links" },
            { "value": "(captain america)" },
            { "value": "avengers has:links" },
            { "value": "avengers" },
            { "value": "(scarlet witch) has:links" },
            { "value": "(scarlet witch)" }
          ],
          "remove": [
            { "value": "new york has:links" }
          ]
        }
      )

      tweetsResponse = requests.get(
        'https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at&expansions=author_id&user.fields=created_at,entities',
        headers=headers,
        stream=True
      )

      # raise an exception if the request is not successful
      print('Requesting tweets...\n')

      if tweetsResponse.status_code != 200:
        raise Exception('Status code is not 200')

      # check if tweets directory exists, if not create it
      if not os.path.exists('tweets'):
        os.mkdir('tweets')

      tweetFileName = 'tweets/tweets_{}.json'.format(self.tweetFileCount)
      tweetFile = open(tweetFileName, 'w')
      tweetFile.write('[')

      # iterate through the tweets and write them to a file
      for tweetInfo in tweetsResponse.iter_lines():
        # Twitter will send a keep-alive ping every 10 seconds, so we will just ignore it
        if tweetInfo == b'':
          print('Keep-alive ping received...\n')
          continue

        tweet = json.loads(tweetInfo)

        # check if we already have saved this tweet and if we have, skip it
        #if tweet['data']['id'] in self.visitedTweetIds:
        #  continue

        # mark the current tweet as visited
        #self.visitedTweetIds.add(tweet['data']['id'])

        # check if the file is too large, if so close the file and create a new one
        if os.path.getsize(tweetFileName) > self.maxTweetFileSize:
          #print(json.dumps(tweet, separators=(',', ':'), indent=2))
          tweetFile.write(json.dumps(tweet, separators=(',', ':')))
          tweetFile.write('\n')
          tweetFile.write(']')
          tweetFile.close()

          self.tweetFileCount += 1
          self.totalTweetFileSize -= self.maxTweetFileSize

          tweetFileName ='tweets/tweets_{}.json'.format(self.tweetFileCount)
          tweetFile = open(tweetFileName, 'w')
          tweetFile.write('[')

        if self.totalTweetFileSize < self.maxTweetFileSize:
          #print(json.dumps(tweet, separators=(',', ':'), indent=2))
          tweetFile.write(json.dumps(tweet, separators=(',', ':')))
          tweetFile.write(']')
          tweetFile.write('\n')
          tweetFile.close()
          print('\nDone!')
          sys.exit(1)

        # if we're not at the end of the tweet file, and were not done retrieving tweets, write the tweet to the file with a comma after it
        #print(json.dumps(tweet, separators=(',', ':'), indent=2))
        tweetFile.write(json.dumps(tweet, separators=(',', ':')))
        tweetFile.write(',')
        tweetFile.write('\n')

    except Exception as e:
      print('Error: ', e)
      sys.exit(0)

if __name__ == '__main__':
  with open('config.json') as json_data_file:
    config = json.load(json_data_file)

    TweetApiRetriever = TweetApiRetriever(config['consumerToken'], config['consumerSecret'])
    TweetApiRetriever.createEncodedTokenSecret()
    TweetApiRetriever.createAccessToken()
    TweetApiRetriever.streamTweets()