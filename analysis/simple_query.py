import pymongo

db = pymongo.MongoClient().network
# db.connection.drop_database('network') DO NOT DO THIS UNLESS YOU WANT TO DROP!!!


# results = db.user_followers.find()
#
# for user in results:
#     print user