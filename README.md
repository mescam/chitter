# chitter
Twitter for Cassandra


## Build docker
```
docker build -t chitter .
```

## Run docker image
```
docker run --name chitter --env-file=credentials.docker -p 8080:8080 -d chitter
```

## Example credentials.docker
```
CASS_CONTACTPOINTS=10.10.10.10,10.10.10.11
CASS_UNAME=cassandra
CASS_PASS=cassandra
CASS_KEYSPACE=chitter
REDIS_DB=0
FLASK_SECRET_KEY=verycomplicatedpassword
REDIS_HOST=10.0.0.13
REDIS_PASS=complicated
```
## Schema
```sql
CREATE KEYSPACE IF NOT EXISTS Chitter
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Chitter;

CREATE TABLE IF NOT EXISTS users (
	username text,
	password text,
	bio text,
	PRIMARY KEY (username)
);

CREATE TABLE IF NOT EXISTS followers_by_user (
    username text,
    follower text,
    PRIMARY KEY(username, follower)
);

CREATE TABLE IF NOT EXISTS following_by_user (
    username text,
    following text,
    PRIMARY KEY(username, following)
);

CREATE TABLE IF NOT EXISTS chitts_by_user (
	username text,
    body text,
    time timeuuid,
    likes int,
    
    p_time int,
    PRIMARY KEY ((username, p_time), time)
) WITH CLUSTERING ORDER BY (time DESC);


CREATE TABLE IF NOT EXISTS chitts_by_follower (
	follower text,
	
	username text,
    body text,
    time timeuuid,
    likes int,
    
    p_time int,
    PRIMARY KEY ((follower, p_time), time)
) WITH CLUSTERING ORDER BY (time DESC);


CREATE TABLE IF NOT EXISTS chitts_by_tag (
	tag text,
	
	username text,
    body text,
    time timeuuid,
    likes int,
    
    p_time int,
    PRIMARY KEY ((tag, p_time), time)
) WITH CLUSTERING ORDER BY (time DESC);


CREATE TABLE IF NOT EXISTS likes_by_user (
    username text,
    time timeuuid,
    p_time int,
    PRIMARY KEY ((username, p_time), time)
) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE IF NOT EXISTS likes_by_chitt (
    time timeuuid,
    username text,
    PRIMARY KEY (time, username)
);

CREATE TABLE IF NOT EXISTS p_time_by_user (
    username text,
    p_time int,
    chitts counter,
    PRIMARY KEY (username, p_time)
) WITH CLUSTERING ORDER BY (p_time DESC);

CREATE TABLE IF NOT EXISTS p_time_by_follower (
    follower text,
    p_time int,
    chitts counter,
    PRIMARY KEY (follower, p_time)
) WITH CLUSTERING ORDER BY (p_time DESC);

CREATE TABLE IF NOT EXISTS p_time_by_tag (
    tag text,
    p_time int,
    chitts counter,
    PRIMARY KEY (tag, p_time)
) WITH CLUSTERING ORDER BY (p_time DESC);
```
