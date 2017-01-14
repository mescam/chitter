from flask_restful import Resource
import db

class Users(Resource):
    def get(self):
        return list(db.Cassandra.gi().users_get()), 200

class User(Resource):
    def get(self, username):
        cass = db.Cassandra.gi()
        user = cass.user_get(username)

        if not user:
            return {}, 404

        followers = cass.followers_by_user_count(username)
        following = cass.following_by_user_count(username)

        return {
            'username': user.username,
            'bio': user.bio,
            'followers': followers,
            'following': following
        }, 200

class UserFollowing(Resource):
    def get(self, username):
        cass = db.Cassandra.gi()
        return list(cass.following_by_user(username))
