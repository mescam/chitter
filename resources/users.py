from flask_restful import Resource
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask import request
from werkzeug.security import generate_password_hash
import db

class Users(Resource):
    def get(self):
        return list(db.Cassandra.gi().users_get()), 200

    def post(self):
        cass = db.Cassandra.gi()
        data = request.json
        uname = data['username'].lower()
        if cass.user_exists(data['username']):
            return {'msg': 'username already exists'}, 409

        password = generate_password_hash(data['password'])
        bio = data.get('bio', None)

        new_user = {
            'password': password,
            'bio': bio
        }

        cass.user_update(uname, new_user)
        return {}, 201

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

    @jwt_required
    def put(self, username):
        if username != get_jwt_identity():
            return {}, 403

        cass = db.Cassandra.gi()
        bio = request.json.get('bio', None)
        password = request.json.get('password', None)

        data = {}
        if bio:
            data['bio'] = bio
        if password:
            data['password'] = generate_password_hash(password)

        cass.user_update(username, data)
        return {}, 204

class UserFollowing(Resource):
    def get(self, username):
        cass = db.Cassandra.gi()
        return list(cass.following_by_user(username))

    @jwt_required
    def post(self, username):
        if username != get_jwt_identity():
            return {}, 403
        
        cass = db.Cassandra.gi()

        who = request.json.get('username')
        if who == username:
            return {'msg': 'forever alone'}, 451

        if not cass.user_exists(who):
            return {}, 404

        cass.follow_user(username, who)
        return {}, 200

    @jwt_required
    def delete(self, username):
        if username != get_jwt_identity():
            return {}, 403
        
        cass = db.Cassandra.gi()
        who = request.json.get('username')
        cass.unfollow_user(username, who)
        return {}, 200

class UserFollowers(Resource):
    def get(self, username):
        cass = db.Cassandra.gi()
        return list(cass.followers_by_user(username))