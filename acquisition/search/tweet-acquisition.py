#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'tkunicki'


import datetime
import json
import os
import sys
import tweepy
import urllib
from tweepy.utils import convert_to_utf8_str


class TweetSerializer:

    out = None
    chunk_count = 0
    tweet_count = 0
    write_method = None

    def __init__(self, basename, chunk_size=1000):
        self.basename = basename
        self.chunk_size = chunk_size
        self.write_method = self.__write_first

    def write(self, tweet):
        self.write_method(tweet)

    def close(self):
        if self.out:
            self.out.write("\n]\n")
            self.out.close()
        self.out = None
        self.write_method = self.__write_first

    def __chunk(self):
        self.close()

    def __write(self, tweet):
        self.out.write(json.dumps(tweet._json).encode('utf8'))
        self.tweet_count += 1

    def __write_first(self, tweet):
        path = self.__next_partition()
        while os.path.isfile(path):
            path = self.__next_partition()
        self.out = open(path, "w")
        self.out.write("[\n")
        self.__write(tweet)
        self.write_method = self.__write_delimited

    def __write_delimited(self, tweet):
        self.out.write(",\n")
        self.__write(tweet)
        if self.tweet_count % self.chunk_size == 0:
            self.__chunk()

    def __next_partition(self):
        path = "%s.%s.json" % (self.basename, self.chunk_count)
        self.chunk_count += 1
        return path


def datetime_partition(start, end, duration):
    current = start
    while start == current or (end - current).days > 0 or\
            ((end - current).days == 0 and (end - current).seconds > 0):
        yield current
        current = current + duration


def date_partition(start, end):
    return datetime_partition(start, end, datetime.timedelta(days=1))


def tweepy_auth(credentials, user=False):
    if user:
        auth = tweepy.OAuthHandler(
            credentials['consumer_key'],
            credentials['consumer_secret'])
        auth.set_access_token(
            credentials['access_token'],
            credentials['access_token_secret'])
        return auth
    else:
        return tweepy.AppAuthHandler(
            credentials['consumer_key'],
            credentials['consumer_secret'])


def tweepy_api(auth):
    api = tweepy.API(
        auth_handler=auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True)
    return api


def tweepy_query(api, q, since=None, until=None, since_id=None):
    return tweepy.Cursor(
        api.search,
        q=urllib.quote_plus(q),
        since=since, until=until, since_id=since_id,
        count=100).items()


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except IOError:
        print "Error loading %s" % path
    except ValueError:
        print "Error parsing %s" % path


def load_credentials(path="credentials.json"):
    return load_json(path)


def string_to_date(date_string):
    try:
        return datetime.datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        pass


def date_to_string(date):
    return date.strftime("%Y-%m-%d")


def format_json(o):
    return json.dumps(o, indent=4, separators=(',', ': '))


def default_queries():
    queries = []
    track = [u'دولة_الخلافة',
             u'الدولة_الإسلامية',
             u'داعش',
             u'جبهة_النصرة',
             u'ولاية_الانبار',
             '#ISIS',
             '#ISIL',
             '#IslamicState']
    for string in track[:-3]:
        queries.append(string + '#')
        queries.append('#' + string)
    for string in track[-3:]:
        queries.append(string)
    return queries


def acquire(request):

    auth = tweepy_auth(credentials)
    api = tweepy_api(auth)

    start = request['start_date']
    end = request['end_date']
    if 'start_index' not in request:
        start_index = 0
    else:
        start_index = int(request['start_index'])

    if 'queries' not in request:
        queries = default_queries()
    else:
        queries = request['queries']

    if 'since_id' not in request:
        since_id = None
    else:
        since_id = request['since_id']

    one_day = datetime.timedelta(days=1)
    try:
        for since_date in date_partition(
                string_to_date(start), string_to_date(end)):
            since = date_to_string(since_date)
            until = date_to_string(since_date + one_day)
            request['start_date'] = since
            for query_index in range(start_index, len(queries)):
                start_index = 0
                request['start_index'] = query_index
                query = convert_to_utf8_str(queries[query_index])
                basename = "tweet_%s_%s" % (query_index, since)
                serializer = TweetSerializer(basename)
                try:
                    print "track_index=%d, q=\"%s\", since=%s, until=%s, since_id=%s" % \
                        (query_index, query, since, until, since_id)
                    count = 0
                    for tweet in tweepy_query(api, query, since, until, since_id):
                        serializer.write(tweet)
                        request['since_id'] = tweet._json['id']
                        count += 1
                    since_id = None
                    print "  %s results" % count
                except:
                    with open("request.json", "w") as request_out:
                        json.dump(request, request_out)
                    raise
                finally:
                    # allows for cleanup on interrupt
                    if serializer:
                        serializer.close()
                request.pop('since_id', None)
            request.pop('start_index', None)
    except KeyboardInterrupt:
        print "Interrupt caught, exiting:..."


def usage():
    print "usage %s start-date [end-date, [recover_index]]" % sys.argv[0]
    print "  start-date, end-date:  YYYY-mm-dd, e.g. \"2015-02-01\""


def usage_credentials():
    print "Twitter API credentials must in a JSON file, \"credentials.json\""
    print "sample format:"
    print format_json({'consumer_key': "XXX",
                       'consumer_secret': "XXX",
                       'access_token': "XXX",
                       'access_token_secret': "XXX"})


def parseCLI():
    argc = len(sys.argv)
    if argc < 2 or argc > 3:
        usage()
        exit(-1)
    start_date = sys.argv[1]
    if not start_date:
        usage()
        exit(-1)
    if argc == 2:
        end_date = start_date
    else:
        end_date = sys.argv[2]
        if not end_date:
            usage()
    return {
        "start_date": start_date,
        "end_date": end_date,
        "queries": default_queries()
    }


if __name__ == "__main__":

    if os.path.isfile("request.json"):
        with open("request.json", "r") as request_in:
            request = json.load(request_in)
    else:
        request = parseCLI()

    credentials = load_credentials()
    if not credentials:
        usage_credentials()
        exit(-1)

    if 'queries' not in request:
        request['queries'] = default_queries()

    acquire(request)
