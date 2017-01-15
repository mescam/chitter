from flask_jwt_extended import create_access_token
from flask_restful import Resource
from flask import request, jsonify
from werkzeug.security import check_password_hash

from db import Cassandra


class Login(Resource):
    def post(self):
        uname = request.json.get('username', None)
        passw = request.json.get('password', None)

        db = Cassandra.gi()
        pattern = db.user_get_password(uname)

        if pattern is not None and check_password_hash(pattern, passw):
            return {
                'access_token': create_access_token(identity=uname)
            }, 200
        else:
            return {'error': 'access denied'}, 401

