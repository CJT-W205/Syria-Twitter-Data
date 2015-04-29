#! /usr/bin/env python
# -*- coding: utf-8 -*-


# pip install Cython
# pip install git+git://github.com/amueller/word_cloud.git
# pip install Image
# pip install python-bidi

import sys
import wordcloud
import arabic_reshaper
from bidi.algorithm import get_display
from unicodedata import bidirectional
import bson
import pymongo


mongo_count_reducer = """
    function (key, values) {
        var total = 0;
        for (var i = 0; i < values.length; i++) {
            total += values[i];
        }
        return total;
    }
"""

mongo_hashtag_mapper = """
    function () {
        this.tags.forEach(function(pair) {
            emit(pair[0], pair[1]);
        });
    }
"""

count_reducer = bson.code.Code(mongo_count_reducer)
hashtag_mapper = bson.code.Code(mongo_hashtag_mapper)


def count_hashtags():
    mongo = pymongo.MongoClient(host="169.53.140.164")
    collection = mongo['stage']['link_analysis']
    collection.map_reduce(hashtag_mapper, count_reducer, "hashtag_counts")
    counts_mr = mongo['stage']['hashtag_counts']
    counts_mr.create_index([('value', pymongo.DESCENDING)])
    counts = counts_mr\
        .find({}, {'value': 1})\
        .sort('value', -1).limit(256)
    counts = map(lambda count: (count['_id'], int(count['value'])), counts)
    cloud(counts, "counts")


def cloud(counts, basename):

    name = basename + ".png"

    wc = wordcloud.WordCloud(
        font_path='/Library/Fonts/Arial.ttf',  # NOTE: font_path is OS X specific
         width=800, height=400)
    wc.fit_words(map(order_and_shape, filter(bad_unicode, counts)))
    wc.to_file(name)
    return name


def order_and_shape(wc):
    return get_display(arabic_reshaper.reshape(wc[0])), wc[1]


def bad_unicode(wc):
    w = wc[0]
    # w = wc[u'_id']
    if not isinstance(w, unicode):
        w = unicode(w)
    prev_surrogate = False
    for _ch in w:
        if sys.maxunicode == 0xffff and (0xD800 <= ord(_ch) <= 0xDBFF):
            prev_surrogate = _ch
            continue
        elif prev_surrogate:
            _ch = prev_surrogate + _ch
            prev_surrogate = False
        if bidirectional(_ch) == '':
            return False
    return True

if __name__ == "__main__":
    count_hashtags()