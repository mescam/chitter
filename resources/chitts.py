from flask_restful import Resource

class Chitt(Resource):

    def get(self):
        return {'lol': 'nie'}
