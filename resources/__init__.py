import resources.users
import resources.chitts

urls = [
    (users.User, '/api/users/'),
    (users.UserFollowing, '/api/users/<string:username>/following'),
    (chitts.Chitt, '/api/chitts/'),
]
