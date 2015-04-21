import json
import random

with open('sample2.json', 'r') as f:
    data = json.load(f)

old_nodes = data['nodes']
old_edges = data['links']

nodes = []
edges = []
counter = 0

for node in old_nodes:
    nodes.append({
        'group': node['group'],
        'id': str(node['name']),
        'x': random.randint(0, 10000),
        'y': random.randint(0, 10000),
        'size': 0.1
    })

for edge in old_edges:
    counter += 1
    edges.append({
        'source': str(edge['source']),
        'target': str(edge['target']),
        'id': str(counter),
        'size': 0.01
    })

newData = {'nodes': nodes, 'edges': edges}

with open('sample3.json', 'w') as f:
    json.dump(newData, f)
    f.close()