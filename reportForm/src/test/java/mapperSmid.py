# -*- coding:utf-8 -*-

import sys
import os
import json



def update_mr_counter(group, counter, amount):
    print('reporter:counter:%s,%s,%d' % (group, counter, amount), file=sys.stderr)

outlier = set([None,"None","NULL",""," ","Null","null","#"])
OS = set(["android","ios"])

try:
    filepath = os.environ['mapreduce_map_input_file']
    filename = os.path.split(filepath)[-1]
except KeyError:
    filepath = os.environ['map_input_file']
    filename = os.path.split(filepath)[-1]

cnt1=0
cnt2=0
cnt3=0
if filename == 'ae':
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            org = obj.get('organization', '#')
            ip = obj.get('data',{}).get('ip','#')
            time = obj.get('timestamp','#')
            ostype = obj.get('features',{}).get('profile.smid_os','#')
            proxyip = obj.get("features",{}).get("profile.proxyip", 0)
            proxyCatchTime = obj.get("features",{}).get("profile.proxyip_catch_time",0)
            if (org not in outlier) and (ip not in outlier) and (ostype in OS):
                cnt1+=1
                key = str(org)+","+str(ip)
                if (proxyip == 1) and (long(str(time))-long(str(proxyCatchTime))<(5184*1000000)):
                    val = str(time)+","+str(1)
                else:
                    val = str(time)+","+str(0)
                print('\t'.join([key, val]))
        except Exception as e:
            update_mr_counter('Mapper', 'Error:' + str(e), 1)

elif filename == 'captcha':
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            org = obj.get('organization', '#')
            ip = obj.get('features',{}).get('data.ip','#')
            time = obj.get('timestamp','#')
            steps = obj.get('step','#')
            ostype = obj.get('features',{}).get('captcha.os','#')
            if (org not in outlier) and (ip not in outlier) and (ostype in OS) and (steps == "sverify"):
                cnt2+=1
                key = str(org)+","+str(ip)
                val = str(time)+","+str(0)
                print('\t'.join([key, val]))
        except Exception as e:
            update_mr_counter('Mapper', 'Error:' + str(e), 1)

else:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            org = obj.get('org', '#')
            ip = obj.get('ip','#')
            time = obj.get('t','#')
            ostype = obj.get('data',{}).get('os','#')
            if (org not in outlier) and (ip not in outlier) and (ostype in OS):
                cnt3+=1
                key = str(org)+","+str(ip)
                val = str(time)+"000"+","+str(0)
                print('\t'.join([key, val]))
        except Exception as e:
            update_mr_counter('Mapper', 'Error:' + str(e), 1)