#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pymongo

colors = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c',
          '#fdbf6f', '#ff7f00', '#cab2d6', '#6a3d9a', '#ffff99']


def read_json(json_file):
    with open(json_file, 'r') as json_reader:
        return json.load(json_reader)


def inject_color(node):
    node['color'] = colors[node['group'] - 1]
    return node


def ingest():
    client = pymongo.MongoClient()
    try:
        staging = client['stage']

        staging.drop_collection('nodes')
        staging['nodes'].insert(map(inject_color, read_json('nodes.json')['nodes']))

        staging.drop_collection('edges')
        staging['edges'].insert(read_json('edges.json'))

    finally:
        client.close()


if __name__ == "__main__":
    ingest()

