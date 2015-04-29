#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pymongo

colors = ['#a6cee3', '#1f78b4', '#b2df8a', '#33a02c', '#fb9a99', '#e31a1c',
          '#fdbf6f', '#ff7f00', '#cab2d6', '#6a3d9a', '#ffff99']


def read_json(json_file):
    with open(json_file, 'r') as json_reader:
        return json.load(json_reader)


def node_scrub(node):
    node['id'] = str(node['id'])
    node['color'] = colors[node['group'] - 1]
    return node


def ingest():
    client = pymongo.MongoClient()
    try:
        staging = client['stage']

        staging.drop_collection('nodes')
        # ensure index
        staging['nodes'].create_index('id')
        staging['nodes'].create_index([('sentiment', pymongo.ASCENDING), ('num_followers', pymongo.ASCENDING)])
        staging['nodes'].insert(map(node_scrub, read_json('nodes.json')['nodes']))

        staging.drop_collection('edges')
        staging['edges'].create_index([('source', pymongo.ASCENDING), ('target', pymongo.ASCENDING)])
        staging['edges'].insert(read_json('edges.json'))

    finally:
        client.close()


def update_coordinates():
    client = pymongo.MongoClient()
    try:
        staging = client['stage']
        # ensure index
        staging['nodes'].create_index('id')
        updates = read_json('nodes_coordinates.json')
        for update in updates['nodes']:
            staging['nodes'].update(
                {'id': update['id']},
                {'$set': {
                    'x': update['x'],
                    'y': update['y']
                }},
            )
    finally:
        client.close()


if __name__ == "__main__":
    ingest()
    update_coordinates()
