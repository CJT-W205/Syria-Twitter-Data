from flask import Flask
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.runner import Runner
import json

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

    # Temporararily getting data from sample3.json
    with open('analysis/sample3.json') as f:
        data = json.load(f)
    nodes = data['nodes']
    edges = data['edges']

    def put(self):
        args = self.parser.parse_args()
        print args
        # {'hashtags': [u'hashtag1', u'hashtag2', u'hashtag3'], 'min_followers': 500,
        # 'isis_group': [u'pro', u'anti', u'neutral', u'eng']}

        # We need to use these args to query Mongo to return a filtered set of nodes and edges, in the format provided
        # in sample3.json (see also sample_transform.py for things like colors used etc. Note that in addition to the
        # existing json attributes in sample3.json, we need to add these filter categories too, obviously!

        return self.data  # Returns the filtered graph data


class UserDetails(Resource):

    @staticmethod
    def get(id):
        print id

        # This API method is to return whatever individual user data we want to show
        # When a node in the graph is clicked on, this API method will be called, and be provided the node/user id as
        # a parameter, to be used in querying to get the response
        # We need to decide if we want to just show user characteristics, recent tweets from the timeline, or something
        # else...

        return {'response': 'this is just a placeholder for the user data'}



# API ROUTING
api.add_resource(Graph, '/graph')
api.add_resource(UserDetails, '/user-details/<int:id>')

if __name__ == "__main__":
    runner.run()