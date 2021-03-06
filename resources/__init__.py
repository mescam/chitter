import resources.users
import resources.chitts
import resources.login

urls = [
	(resources.login.Login, '/api/login'),
    (resources.users.Users, '/api/users'),
    (resources.users.User, '/api/users/<string:username>'),
    (resources.users.UserFollowing, '/api/users/<string:username>/following'),
    (resources.users.UserFollowers, '/api/users/<string:username>/followers'),
    (resources.chitts.Chitts, '/api/chitts'),
    (resources.chitts.ChittsLike, '/api/chitts/<string:author>,<string:timeuuid>/likes')
]
