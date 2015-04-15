#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'tkunicki'

import pymongo
import glob
import json
import sys


def list_tweet_files(file_pattern="tweet_*_*.json"):
    return glob.glob(file_pattern)


def tweets(tweet_files=list_tweet_files()):
    for tweet_file in tweet_files:
        print "reading '%s'" % tweet_file
        with open(tweet_file, 'r') as tweet_reader:
            for line in tweet_reader:
                if len(line) > 2:
                    if line[-2] == ',':
                        line = line[:-2]
                    else:
                        line = line[:-1]
                    yield json.loads(line)


def create(client, db="tweets", collection="search"):
    client[db][collection].create_index([("id", pymongo.ASCENDING)], unique=True)


def ingest(ingest_tweets=tweets(), host="localhost", port=27017, db="tweets", collection="search"):
    client = pymongo.MongoClient(host, port)
    create(client, db, collection)
    try:
        collection = client[db][collection]
        try:
            collection.insert(ingest_tweets, continue_on_error=True)
        except pymongo.errors.DuplicateKeyError:
            pass
    finally:
        client.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    if argc > 1:
        ingest_tweets = tweets(list_tweet_files(sys.argv[1]))
    else:
        ingest_tweets = tweets()
    ingest(ingest_tweets)

