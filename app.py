from flask import Flask
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.runner import Runner
import json
import pymongo
import bson.json_util
import bson


app = Flask(__name__)
runner = Runner(app)
api = Api(app)

# Enable CORS
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


class Graph(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('min_followers', type=int, required=True)
    parser.add_argument('hashtags', type=list, location='json', required=True)
    parser.add_argument('isis_group', type=list, location='json', required=True)

    # # Temporarily getting data from sample3.json
    # with open('analysis/sample3.json') as f:
    #     data = json.load(f)
    # nodes = data['nodes']
    # edges = data['edges']

    def put(self):
        args = self.parser.parse_args()
        print args
        # {'hashtags': [u'hashtag1', u'hashtag2', u'hashtag3'], 'min_followers': 500,
        # 'isis_group': [u'pro', u'anti', u'neutral', u'eng']}

        # We need to use these args to query Mongo to return a filtered set of nodes and edges, in the format provided
        # in sample3.json (see also sample_transform.py for things like colors used etc. Note that in addition to the
        # existing json attributes in sample3.json, we need to add these filter categories too, obviously!
        nodes = mongo['stage']['nodes'].find(
            {'$and': [
                {'sentiment': {'$in': args['isis_group']}},
                # {'tags': {'$in': args['hashtags']}},
                {'followers_count': {'$gte': args['min_followers']}}
            ]},
            {'_id': 0})
        nodes = [self.node_scrub(node, index) for index, node in enumerate(nodes)]

        nodes_id = list(set([node['id'] for node in nodes]))

        edges = mongo['stage']['edges'].find(
            {'$and': [
                {'source': {'$in': nodes_id}},
                {'target': {'$in': nodes_id}}]},
            {'_id': 0})
        edges = [self.edge_scrub(edge, index) for index, edge in enumerate(edges)]

        result = {'nodes': nodes, 'edges': edges}
        return result

    @staticmethod
    def node_scrub(node, index):
        node['id'] = str(node['id'])
        return node

    @staticmethod
    def edge_scrub(edge, index):
        edge['id'] = index
        return edge


class UserDetails(Resource):

    @staticmethod
    def get(id):

        # This API method is to return whatever individual user data we want to show
        # When a node in the graph is clicked on, this API method will be called, and be provided the node/user id as
        # a parameter, to be used in querying to get the response
        # We need to decide if we want to just show user characteristics, recent tweets from the timeline, or something
        # else...

        result = mongo['stage']['views'].find(
            {'_id': id},
            {'_id', 0})
        return result #mongo_convert(result)


def mongo_convert(o):
    # need more efficient way of doing this...
    json.loads(bson.json_util.dumps(o))


# API ROUTING
api.add_resource(Graph, '/graph')
api.add_resource(UserDetails, '/user-details/<int:id>')

# MongoDB
mongo = pymongo.MongoClient(host="localhost", port=27017)

if __name__ == "__main__":
    runner.run()