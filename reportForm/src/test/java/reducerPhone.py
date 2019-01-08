#-*-coding:utf-8-*-


import sys
import json

def update_mr_counter(group, counter, amount):
    print('reporte:counter:%s, %s, %d' %(group, counter, amount), file=sys.stderr)


def initRecord(key, val):
    record = {
        "org":key.split(",")[0],
        'phone':key.split(",")[1],
        "time":val.split(",")[0],
        "phoneblack":val.split(",")[1]
    }
    return record

last_key=None
last_record={}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key = line.split("\t")[0]
        val = line.split("\t")[1]
        s = val.split(",")
        if key == last_key:
            if s[0] > last_key["time"]:
                last_record["time"] = s[0]
            if s[1] == "1":
                last_record["phoneblack"]=s[1]
        else:
            if last_key != None:
                print(json.dumps(last_record))
            last_record=initRecord(key, val)
            last_key = key
    except Exception as e:
        update_mr_counter('Reducer', 'Error' + str(e), 1)

if last_key != None:
    print(json.dumps(last_record))