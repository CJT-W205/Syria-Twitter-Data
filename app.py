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
    parser.add_argument('clusters', type=list, location='json')
    with open('miserables.json') as f:
        data = json.load(f)
    nodes = data['nodes']
    links = data['links']

    def get(self):
        groups = set()
        for node in self.nodes:
            groups.add(node['group'])
        return {'groups': list(groups)}  # Returns the available groups

    def put(self):
        args = self.parser.parse_args()
        groups = args['clusters']
        filtered_nodes = []
        filtered_links = []
        translate = {}
        ids = set()
        nodes = self.nodes[:]
        for i, node in enumerate(nodes):
            if node['group'] in groups:
                filtered_nodes.append(node)
                translate[i] = (len(filtered_nodes)-1)  # to translate the current array references to new ones
                ids.add(i)

        links = self.links[:]
        for link in links:
            if link['source'] in ids and link['target'] in ids:
                link['source'] = translate[link['source']]  # translate the array references
                link['target'] = translate[link['target']]
                filtered_links.append(link)

        response = {'data': {
            'nodes': filtered_nodes,
            'links': filtered_links
        }}
        return response  # Returns the filtered graph data




# API ROUTING
api.add_resource(Graph, '/graph')


if __name__ == "__main__":
    runner.run()