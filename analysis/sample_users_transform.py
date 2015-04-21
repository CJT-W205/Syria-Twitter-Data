import json
import random

with open('sample_users.json', 'r') as f:
    data = json.load(f)

old_users = data['users']

users = {}

for user in old_users:
    users[user['_id']] = {'isis': user['isis'],
                          'screen_name': user['screen_name'],
                          'following_count': user['following_count']}


with open('users_dict.json', 'w') as f:
    json.dump(users, f)
    f.close()