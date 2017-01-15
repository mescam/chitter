from flask_restful import Resource
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask import request
import db

MAX_CHITTS_COUNT = 50
MAX_P_TIMES_COUNT = 3

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


def format_response(next_p_time, chitts):
    return {
        'next_p_time': next_p_time,
        'chitts': chitts
    }


class Chitts(Resource):
    def get(self):
        filter_type = request.args.get('type') or 'public'
        upper_bound = request.args.get('p_time') or 9999999
        keyword = request.args.get('keyword') or ''

        p_times, next_p_time = get_p_times(
                filter_type,
                keyword,
                int(upper_bound)
            )        
        chitts = list(cass.chitts_by(filter_type, keyword, p_times))

        return format_response(next_p_time, chitts), 200

