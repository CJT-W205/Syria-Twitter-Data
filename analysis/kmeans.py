def parseVector(line):
    return json.loads(line.replace("'", "\""))

user_dict = {}

def user_index(user_dict, user):
  	if user not in user_dict:
  		user_index = len(user_dict)
  		user_dict[user] = user_index
  		return user_index
  	else:

  		return user_dict[user]


def buildSparseVectorByUser(user, n):
    return SparseVector(n,
                        dict(                              \
                        (userIndex(user_dict, j[0]), j[1]) \
                        for j in user[1]))
