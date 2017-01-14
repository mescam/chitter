import resources.users
import resources.chitts
import resources.login

urls = [
	(resources.login.Login, '/api/login'),
    (resources.users.Users, '/api/users'),
    (resources.users.User, '/api/users/<string:username>'),
    (users.UserFollowing, '/api/users/<string:username>/following'),
    (chitts.Chitt, '/api/chitts/'),
]
