from flask_restful import Resource
import db

class User(Resource):
    def get(self):
        return {'test': 'test'}

class UserFollowing(Resource):
    def get(self, username):
        cass = db.Cassandra.gi()
        return list(cass.following_by_user(username))
