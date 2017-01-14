import re

_HASHTAG_RE = re.compile(r"\#([A-Za-z0-9]+)", re.M)

def parse_tags(text):
    return _HASHTAG_RE.findall(text)

def partition_time(time):
    return '-'.join(map(str, time.isocalendar()[:2]))

if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'tags':
        print(parse_tags("nie wiem co powiedziec #srds2016"))
        print(parse_tags("kocham #sr #pp"))
        print(parse_tags("#notice-me-senpai #systemy #rozproszone #nie_wiem"))
    elif sys.argv[1] == 'hash':
        from werkzeug.security import generate_password_hash
        print(generate_password_hash(sys.argv[1]))

