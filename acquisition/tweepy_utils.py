#!/usr/bin/python
from tweepy import OAuthHandler, AppAuthHandler, API
import os
import json


def format_json(o):
    return json.dumps(o, indent=4, separators=(',', ': '))


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except IOError:
        print "Error loading %s" % path
    except ValueError:
        print "Error parsing %s" % path


def load_credentials(from_file=False, path='credentials.json'):
    if from_file:
        try:
            with open(path) as f:
                credentials = json.load(f)
        except (IOError, ValueError):
            print "Your twitter API credentials must be available in a JSON file, \"credentials.json\""
            print "sample format:"
            print format_json({'consumer_key': "XXX",
                               'consumer_secret': "XXX",
                               'access_token': "XXX",
                               'access_token_secret': "XXX"})
            credentials = None
        return credentials
    else:
        return {'consumer_key': os.environ['TW_CONS_KEY'],
                'consumer_secret': os.environ['TW_CONS_SECRET'],
                'access_token': os.environ['TW_AT_KEY'],
                'access_token_secret': os.environ['TW_AT_SECRET']}


def tweepy_auth(credentials, user=False):
    if user:
        auth = OAuthHandler(credentials['consumer_key'], credentials['consumer_secret'])
        auth.set_access_token(credentials['access_token'], credentials['access_token_secret'])
        return auth
    else:
        return AppAuthHandler(credentials['consumer_key'], credentials['consumer_secret'])


def tweepy_api(auth):
    api = API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api