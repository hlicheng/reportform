# -*- coding:utf-8 -*-


############----------TokenId-------------##################


import sys
import json
import os

def update_mr_counter(group, counter, amount):
    print('reporter:counter:%s,%s,%d' % (group, counter, amount), file=sys.stderr)

count=0
count2=0

outlier = set([None,"None","NULL",""," ","Null","null","#"])
OS = set(["android","ios"])

try:
    filepath = os.environ['mapreduce_map_input_file']
    filename = os.path.split(filepath)[-1]
except KeyError:
    filepath = os.environ['map_input_file']
    filename = os.path.split(filepath)[-1]

if filename=="ae":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            org=obj.get("organization", "#")
            tokenid=obj.get("data",{}).get("tokenId","#")
            timestamp=obj.get("timestamp", "#")
            score=obj.get("features",{}).get("profile.token_score_v1",0)
            ostype=obj.get("features",{}).get("profile.smid_os","#")
            if (org not in outlier) and (tokenid not in outlier) and (ostype in OS):
                count+=1
                key = str(org)+','+str(tokenid)
                if (score >= 700):
                    val = str(timestamp)+','+str(1)
                else:
                    val = str(timestamp)+','+str(0)
                print("\t".join([key, val]))
                # print(count, file=sys.stderr)
        except Exception as e:
            update_mr_counter('Mapper', 'Error:' + str(e), 1)

else:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            count2+=1
            org=obj.get("organization", "#")
            tokenid=obj.get("features",{}).get("data.tokenId","#")
            timestamp=obj.get("timestamp", "#")
            ostype=obj.get("features",{}).get("captcha.os")
            steps=obj.get("step","#")
            if (org not in outlier) and (tokenid not in outlier) and (ostype in OS) and (steps == "sverify"):
                key = str(org)+","+str(tokenid)
                val = str(timestamp)+","+str(0)
                print("\t".join([key, val]), file=sys.stderr)
        except Exception as e:
            update_mr_counter('Mapper', 'Error:' + str(e), 1)