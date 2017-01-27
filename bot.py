#!/usr/bin/env python
import requests
import json
import multiprocessing as mp
import threading as t
import random

URL = "http://node01.aws.mescam.pl"

usernames = ["levi","bling","frisco","gator","bailey","shiloh","gatsby","chester","ginger","cedric","nova","grace","storm","tripp","brandy","yoda","trooper","whip","juno","buzz","emmy","kobe","brutus","chili","hannah","cole","summer","obie","gator","leo"]
tokens = {}

hashtags = ["#icehockey", "#boxer", "#lacrosse", "#shortstop", "#taekwondo", "#huddle", "#cue", "#canoeing", "#shotput", "#sled", "#relay", "#bowling", "#forward", "#unicycle", "#dart", "#rink", "#running", "#jump", "#scuba", "#handball", "#mitt", "#kickball", "#skate", "#soccer", "#halftime"]

with open('quotes.csv', 'r') as f:
    quotes = f.read().splitlines()


def get_token(username, password):
    d = requests.post(URL+'/api/login',
        json={
            'username': username,
            'password': password
        })

    tokens[username] = d.json()['access_token']

def send_chitt(token, body):
    d = requests.post(URL+'/api/chitts',
        json={
            'body': body
        },
        headers={
            'Authorization': 'Bearer %s' % token
        })

def register_user(username, password):
    print "Registering %s" % username
    requests.post(URL + '/api/users',
        json={
            'username': username,
            'password': password
        })

def follow_user(follower, follows, token):
    requests.post(URL + '/api/users/%s/following' % follower,
        json={
            'username': follows
        },
        headers={
            'Authorization': 'Bearer %s' % token
        })

def get_chitts():
    return requests.post(URL + '/api/chitts').json()

def like(author, uuid, token):
    requests.post(URL + '/api/chitts/%s,%s/likes' % (author, uuid),
        headers={
            'Authorization': 'Bearer %s' % token
        })

def follow_users(username):
    tf = random.randint(0,20)
    fs = random.sample(usernames, k=tf)
    print 'User %s will follow %s' % (username, fs)
    for f in fs:
        follow_user(username, f, tokens[username])

def chitt_things(username):
    cc = random.randint(300, 1000)
    for i in xrange(cc):
        q = random.choice(quotes)

        ch = random.randint(0, 5)
        hs = random.sample(hashtags, ch)
        q = '%s %s' % (' '.join(hs), q)
        send_chitt(tokens[username], q)

def execute(ps):
    for p in ps:
        p.start()
    for p in ps:
        p.join()

print "Register users"
process_register = []
for u in usernames:
    process_register.append(mp.Process(target=register_user, args=(u, 'test')))

execute(process_register)


print "Get tokens"
process_token = []
for u in usernames:
    process_token.append(t.Thread(target=get_token, args=(u, 'test')))

execute(process_token)


print "Follow users"
process_follow = []
for u in usernames:
    process_follow.append(mp.Process(target=follow_users, args=(u,)))
execute(process_follow)


print "Chitt things"
process_chitt = []
for u in usernames:
    process_chitt.append(mp.Process(target=chitt_things, args=(u,)))
execute(process_chitt)

