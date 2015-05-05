from flask import Flask
from flask import send_file
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.runner import Runner
import abc
import json
import pymongo
import bson.json_util
import bson
import analysis.word_cloud
import StringIO


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


class NodeResource(Resource):

    kvp = reqparse.RequestParser()
    kvp.add_argument('min_followers', type=int, location='args')
    kvp.add_argument('hashtags', type=str, action='append', location='args')
    kvp.add_argument('isis_group', type=str, action='append', location='args')
    kvp.add_argument('group', type=str, action='append', location='args')

    json = reqparse.RequestParser()
    json.add_argument('min_followers', type=int, location='json')
    json.add_argument('hashtags', type=list, location='json')
    json.add_argument('isis_group', type=list, location='json')
    json.add_argument('group', type=list, location='json')

    @staticmethod
    def node_query(args):

        and_clause = []

        tags = args.get('hashtags', None)
        if tags is not None and len(tags) > 0:
            and_clause.append({'tags': {'$in': tags}})

        follower_count = args.get('min_followers', None)
        if follower_count is not None:
            and_clause.append({'followers_count': {'$gte': follower_count}})

        sentiment = args.get('isis_group', None)
        if sentiment is not None and len(sentiment) > 0:
            and_clause.append({'sentiment': {'$in': sentiment}})

        group = args.get('group', None)
        if group is not None and len(group) > 0:
            and_clause.append({'group': {'$in': group}})

        clause_count = len(and_clause)
        if clause_count > 1:
            return {'$and': and_clause}
        else:
            return and_clause[0] if clause_count == 1 else {}

    def args_scrub(self, args):
        args = args.copy()
        self.value_scrub(args, 'hashtags')
        self.value_scrub(args, 'isis_group')
        self.value_scrub(args, 'group', lambda g: int(g))
        return args

    @staticmethod
    def value_scrub(args, key, conv=None):
        value = args.get(key, None)
        if value is not None:
            if isinstance(value, list):
                value = map(lambda v: v.split(","), value)
                value = [item for sublist in value for item in sublist]
            else:
                value = value.split(",")
            if conv is not None:
                value = map(conv, value)
            args[key] = list(set(value))

    def get(self):
        args = self.kvp.parse_args()
        args = self.args_scrub(args)
        node_query = self.node_query(args)
        return self.handle_request(node_query)

    def put(self):
        args = self.json.parse_args()
        node_query = self.node_query(args)
        return self.handle_request(node_query)

    def post(self):
        args = self.json.parse_args()
        node_query = self.node_query(args)
        return self.handle_request(node_query)

    @abc.abstractmethod
    def handle_request(self, node_query):
        pass


class Graph(NodeResource):

    def handle_request(self, node_query):

        if not node_query:
            # protect against empty query (expensive!)
            nodes = {}
            edges = {}
        else:
            nodes = mongo['stage']['nodes'].find(node_query, {'_id': 0})
            nodes = [self.node_scrub(node, index) for index, node in enumerate(nodes)]

            nodes_id = list(set([node['id'] for node in nodes]))

            edges = mongo['stage']['edges'].find(
                {'$and': [
                    {'source': {'$in': nodes_id}},
                    {'target': {'$in': nodes_id}}]},
                {'_id': 0})
            edges = [self.edge_scrub(e, i) for i, e in enumerate(edges)]

        return {'nodes': nodes, 'edges': edges}

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


class WordCloud(NodeResource):

    def handle_request(self, node_query):

        collection = mongo['stage']['nodes']

        counts = analysis.word_cloud.count_hashtags(collection, node_query)

        image = analysis.word_cloud.cloud_image(counts)
        image_io = StringIO.StringIO()
        image.save(image_io, "png")
        image_io.seek(0)
        return send_file(image_io,
                         # attachment_filename="cloud.png",
                         # as_attachment=True,
                         mimetype='image/png')


class UserDetails(Resource):

    @staticmethod
    def get(id):
        result = mongo['stage']['views'].find({'_id': id})
        return json.loads(bson.json_util.dumps(result))


# API ROUTING
api.add_resource(Graph, '/graph')
api.add_resource(UserDetails, '/user-details/<int:id>')
api.add_resource(WordCloud, '/word-cloud')

# MongoDB
mongo = pymongo.MongoClient(host="169.53.140.164", port=27017)

if __name__ == "__main__":
    runner.run()
