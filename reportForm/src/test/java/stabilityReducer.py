# -*- coding:utf-8 -*-


import sys
import json


def update_mr_counter(group, counter, amount):
    print("reporter:counter:%s,%s,%d" % (group, counter, amount))


def calculatePerOrg(val, map):
    cost = float(val.split(",")(0))
    istimeout = int(val.split(",")(1))
    uricnt = int(val.split(",")(2))
    _cost = float(map['cost']) + cost
    _timeoutcnt = int(map['timeoutcnt']) + istimeout
    _uricnt = int(map['uricnt']) + uricnt
    map['cost'] =  _cost
    map['timeoutcnt'] = _timeoutcnt
    map['uricnt'] = _uricnt
    return map

def initMap(key, val):
    map = {
        "uri":key.split(",")(0),
        "org":key.split(",")(1),
        "cost":val.split(",")(0),
        "uricnt":val.split(",")(2),
        "timeoutcnt":val.split(",")(1)
    }
    return map

lastkey = None
map = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key = line.split("\t")(0)
        val = line.split("\t")(1)
        if lastkey == key:
            calculatePerOrg(key, val)
        else:
            if lastkey != None:
                cost = float(map['cost'])
                uricnt = int(map['uricnt'])
                avgcost = float(cost/uricnt)
                map['cost'] = avgcost
                print(json.dumps(map))
            map = initMap(key, val)
            lastkey = key

    except Exception as e:
        update_mr_counter("Reducer", "Error:" + str(e), 1)

if lastkey != None:
    print(json.dumps(map))