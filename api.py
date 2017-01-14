import os

from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from flask_jwt_extended import JWTManager

from db import Cassandra
import resources

app = Flask(__name__)
app.secret_key = os.environ['FLASK_SECRET_KEY']
api = Api(app)
jwt = JWTManager(app)
db = Cassandra.gi()



class WelcomeResource(Resource):
    def get(self):
        return {'error': 'not found'}, 404

for (cls, endpoint) in resources.urls:
    print(cls, endpoint)
    api.add_resource(cls, endpoint)

if __name__ == "__main__":
    app.run()
