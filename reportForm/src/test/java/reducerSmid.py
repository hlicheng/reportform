# -*- coding:utf-8 -*-

import sys
import json

def update_mr_counter(group, counter, amount):
    print("reporter:counter:%s,%s,%d" % (group, counter, amount))


def initrecord(key, val):
    last_record = {
        "org":key.split(",")[0],
        "ip":key.split(",")[1],
        "time":val.split(",")[0],
        "isblack":val.split(",")[1]
    }
    return last_record

last_key = None
lastrecord = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key = line.split("\t")[0]
        val = line.split("\t")[1]
        s = val.split(",")
        if key == last_key:
            if s[0] > lastrecord["time"]:
                lastrecord["time"] = s[0]
            if s[1] == "1":
                lastrecord["isblack"]=s[1]
        else:
            if last_key != None:
                print(json.dumps(lastrecord))
            lastrecord = initrecord(key, val)
            last_key = key
    except Exception as e:
        update_mr_counter("Mapper", "Error:" + str(e), 1)
if last_key != None:
    print(json.dumps(lastrecord))
