# -*- coding:utf-8 -*-

import sys
import json

def update_mr_count(group, conter, amount):
    print('reporter:counter:%s,%s,%d' % (group, counter, amount), file=sys.stderr)

map = {}
try:
    with open('uri', 'r') as f:
        line = f.readline()
        if not line:
            uri = line.split("\t")(0)
            time = float(line.split("\t")(1))
            map[uri] = time
except Exception as e:
    print(e)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        s = line.split("\t")
        uri = s[4].split("=")[1]
        cost = float(s[-1].split("=")[1])
        if len(s) <= 8:
            jsonstr = s[6].split("=")[1]
            obj = json.loads(jsonstr)
            organization = obj.get("organization","#")
        else:
            organization = s[6].split("=")[1]
        try:
            if map[uri] < cost:
                istimeout = 0
            else:
                istimeout = 1
        except KeyError:
            if cost > 4000:
                istimeout = 1
            else:
                istimeout = 0
        key1 = str(uri)+","+str(organization)
        key2 = str(uri)+","+str("all")
        val = str(cost)+","+str(istimeout)+","+str(1)
        print("\t".join([key1, val]))
        print("\t".join([key2, val]))


    except Exception as e:
        update_mr_count('Mapper', 'Error:' + str(e), 1)
