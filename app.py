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

    def put(self):
        args = self.parser.parse_args()

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
        node['size'] = 0.01
        node['label'] = node['id']
        return node

    @staticmethod
    def edge_scrub(edge, index):
        edge['id'] = index
        return edge


class UserDetails(Resource):

    @staticmethod
    def get(id):
        result = mongo['stage']['views'].find({'_id': id})
        return json.loads(bson.json_util.dumps(result))


# API ROUTING
api.add_resource(Graph, '/graph')
api.add_resource(UserDetails, '/user-details/<int:id>')

# MongoDB
mongo = pymongo.MongoClient(host="169.53.140.164", port=27017)

if __name__ == "__main__":
    runner.run()