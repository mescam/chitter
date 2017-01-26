from flask_restful import Resource
from flask_jwt_extended import get_jwt_identity, jwt_required, get_raw_jwt
from flask import request
import bleach

import db
from utils import jwt_barrier

MAX_CHITTS_COUNT = 100
MAX_P_TIMES_COUNT = 3
MAX_CHITT_LEN = 128

cass = db.Cassandra.gi()

def get_p_times(filter_type, keyword, upper_bound, ):
    p_times = list(cass.p_times_by(
        filter_type,
        keyword,
        upper_bound,
        MAX_P_TIMES_COUNT + 1)
    )
    if len(p_times) == 0:
        return [], None

    limited = [p_times[0][0]]
    count_sum = p_times[0][1]
    for p_t, count in p_times[1:MAX_P_TIMES_COUNT]:
        if count_sum >= MAX_CHITTS_COUNT:
            return limited, p_t
        limited.append(p_t)
        count_sum += count

    try:
        return limited, p_times[MAX_P_TIMES_COUNT][0]
    except IndexError:
        return limited, None


def format_response(next_p_time, chitts, status):
    return {
        'next_p_time': next_p_time,
        'chitts': chitts,
        'completeResponse': status
    }


class Chitts(Resource):
    def get(self):
        filter_type = request.args.get('type') or 'public'
        upper_bound = request.args.get('p_time') or 9999999
        keyword = request.args.get('keyword') or ''
        last_chitt_ubound = request.args.get('last_chitt_ubound', None)

        if jwt_barrier(True):
            username = get_jwt_identity()
        else:
            username = None

        if filter_type == 'follower':
            if username:
                keyword = username
            else:
                return {}, 403

        p_times, next_p_time = get_p_times(
                filter_type,
                keyword,
                int(upper_bound)
            )

        status, chitts, npt = cass.chitts_by(filter_type, 
                                             keyword,
                                             p_times, 
                                             last_chitt_ubound)
        if npt:
            next_p_time = npt

        if username:
            likes = list(cass.likes_by_user(username, p_times))
            if likes:
                i, j = 0, 0
                for i in range(len(chitts)):
                    if chitts[i]['id'] == str(likes[j].time):
                        chitts[i]['liked'] = True
                        j += 1
                        if j == len(likes):
                            break

        return format_response(next_p_time, chitts, status), 200

    @jwt_required
    def post(self):
        body = request.json.get('body', None)

        if body is None:
            return {'error': 'no body provided'}, 400
        elif len(body) > MAX_CHITT_LEN:
            return {'error': 'chitt body too long'}, 413

        cass.chitt_add(
            get_jwt_identity(),
            body
        )
        return {}, 200

class ChittsLike(Resource):
 
    @jwt_required
    def post(self, author, timeuuid):
        if not cass.chitt_exists(author, timeuuid):
            return {}, 404
        username = get_jwt_identity()
        cass.like_add(username, author, timeuuid)
        return {}, 201

    @jwt_required
    def delete(self, author, timeuuid):
        if not cass.chitt_exists(author, timeuuid):
            return {}, 404
        username = get_jwt_identity()
        cass.like_delete(username, author, timeuuid)
        return {}, 200


    def get(self, author, timeuuid):
        if not cass.chitt_exists(author, timeuuid):
            return {}, 404
        return {'likes': list(cass.likes_by_chitt(timeuuid))}, 200
