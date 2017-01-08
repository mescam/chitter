import re

_HASHTAG_RE = re.compile(r"\#([A-Za-z0-9]+)", re.M)

def parse_tags(text):
    return _HASHTAG_RE.findall(text)

def partition_time(time):
	return '-'.join(map(str, time.isocalendar()[:2]))

if __name__ == '__main__':
    print(parse_tags("nie wiem co powiedziec #srds2016"))
    print(parse_tags("kocham #sr #pp"))
    print(parse_tags("#notice-me-senpai #systemy #rozproszone #nie_wiem"))

