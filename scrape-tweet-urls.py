import scrapy
import ijson
import os
import sys
import json

class TweetURLScraper(scrapy.Spider):
  name = 'tweeturlscraper'

  def __init__(self, fileCount=1, **kwargs):
    self.currentFileNumber = 1
    self.fileCount = int(fileCount)
    super().__init__(**kwargs)

  def start_requests(self):
    while self.currentFileNumber <= self.fileCount:
      fileExists = os.path.isfile('tweets/tweets_{}.json'.format(self.currentFileNumber))

      if not fileExists:
        print('The tweet file (tweets/tweets_{}.json) does not exist'.format(self.currentFileNumber))
        sys.exit(0)

      with open('tweets/tweets_{}.json'.format(self.currentFileNumber)) as f:
        for tweet in ijson.items(f, "item"):
          # go through all users within a tweet
          for userIndex, user in enumerate(tweet['includes']['users']):
            # check if the tweet contains a URL
            if 'entities' in user and 'url' in user['entities']:
              for urlIndex in range(len(user['entities']['url'])):
                yield scrapy.Request(user['entities']['url']['urls'][urlIndex]['expanded_url'], self.parse, cb_kwargs={
                  'tweet': tweet,
                  'userIndex': userIndex,
                  'urlIndex': urlIndex,
                  # we have to pass the current file number because the self.currentFileNumber will increment before all tweets make it to the current file
                  'fileNumber': self.currentFileNumber
                })

      self.currentFileNumber += 1

  def parse(self, response, tweet=None, userIndex=None, urlIndex=None, fileNumber=None):
    with open('tweets/tweets_{}_with_titles.json'.format(fileNumber), 'a+') as tweetFile:
      title = response.css('title::text').get().strip()

      if title != None:
        tweet['includes']['users'][userIndex]['entities']['url']['urls'][urlIndex]['title'] = title

        # write the tweet to the file
        tweetFile.write(json.dumps(tweet, indent=2) + '\n')
        tweetFile.write(',')