from flask import Flask
from flask_restful import Resource, Api

from db import Cassandra
import resources

app = Flask(__name__)
api = Api(app)
db = Cassandra.gi()

class WelcomeResource(Resource):
    def get(self):
        return {'error': 'not found'}, 404

for (cls, endpoint) in resources.urls:
    api.add_resource(cls, endpoint)

if __name__ == "__main__":
    app.run()
