import re

from flask_jwt_extended.config import get_blacklist_enabled
from flask_jwt_extended.exceptions import WrongTokenError
from flask_jwt_extended.blacklist import check_if_token_revoked
from flask_jwt_extended.utils import _decode_jwt_from_request

try:
    from flask import _app_ctx_stack as ctx_stack
except ImportError:  # pragma: no cover
    from flask import _request_ctx_stack as ctx_stack

_HASHTAG_RE = re.compile(r"\#([A-Za-z0-9]+)", re.M)

def parse_tags(text):
    return _HASHTAG_RE.findall(text)

def partition_time(time):
    # return int(time.timestamp() / (3600 * 24 * 7))
    return int(time.timestamp() / (3600))

def jwt_barrier(silent=False):
    try:
        jwt_data = _decode_jwt_from_request(type='access')

        if jwt_data['type'] != 'access':
            raise WrongTokenError('Only access tokens can access this endpoint')

        blacklist_enabled = get_blacklist_enabled()
        if blacklist_enabled:
            check_if_token_revoked(jwt_data)

        ctx_stack.top.jwt_identity = jwt_data['identity']
        ctx_stack.top.jwt_user_claims = jwt_data['user_claims']
        ctx_stack.top.jwt = jwt_data
        return True
    except Exception as e:
        if not silent:
            raise e
        else:
            return False

if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'tags':
        print(parse_tags("nie wiem co powiedziec #srds2016"))
        print(parse_tags("kocham #sr #pp"))
        print(parse_tags("#notice-me-senpai #systemy #rozproszone #nie_wiem"))
    elif sys.argv[1] == 'hash':
        from werkzeug.security import generate_password_hash
        print(generate_password_hash(sys.argv[2]))

