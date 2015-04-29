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

def cloud(counts, basename):

    name = basename + ".png"

    count_max = counts.most_common()[0][1]
    if count_max == 0:
        return

    # NOTE: font_path is OS X specific
    wc = wordcloud.WordCloud(font_path='/Library/Fonts/Arial.ttf',
                             width=800, height=400)
    wc.fit_words(map(order_and_shape, filter(bad_unicode, counts.most_common(256))))
    wc.to_file(name)
    return name


def order_and_shape(wc):
    return get_display(arabic_reshaper.reshape(wc[0])), wc[1]


def bad_unicode(wc):
    w = wc[0]
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