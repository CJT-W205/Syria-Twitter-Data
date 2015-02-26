import pymongo

db = pymongo.MongoClient().network
db.connection.drop_database('network')


# results = db.user_followers.find()
#
# for user in results:
#     print user