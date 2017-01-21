import os
from datetime import timedelta

from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from flask_jwt_extended import JWTManager

from flask_cors import CORS, cross_origin

from db import Cassandra
import resources

app = Flask(__name__)
app.secret_key = os.environ['FLASK_SECRET_KEY']
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(days=365)

CORS(app)

api = Api(app)
jwt = JWTManager(app)
db = Cassandra.gi()

for (cls, endpoint) in resources.urls:
    print(cls, endpoint)
    api.add_resource(cls, endpoint)

if __name__ == "__main__":
    app.run()
