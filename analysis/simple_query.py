import pymongo

db = pymongo.MongoClient().network
# results = db.user_followers.find()
#
# for user in results:
#     print user

# db.connection.drop_database('test')
# db.connection.drop_database('user_profiles')
db.connection.drop_database('network')
