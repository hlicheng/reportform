# -*- coding:utf-8 -*-

import json
import sys

def update_mr_counter(group, counter, amount):
    print('reporter:counter:%s,%s,%d' %(group,counter,amount), file=sys.stderr)


outlier=set([None,"None","NULL",""," ","Null","null","#"])
OS=set(["android","ios"])
for line in sys.stdin:
    line=line.strip()
    if not line:
        continue
    try:
        obj=json.loads(line)
        org=obj.get("organization","#")
        phone=obj.get("features",{}).get("profile.phone","#")
        time=obj.get("timestamp","#")
        phoneblack=obj.get("features",{}).get("profile.phone_i_histo_black","#")
        ostype=obj.get("features",{}).get("profile.smid_os","#")
        if (org not in outlier) and (phone not in outlier) and  (ostype in OS):
            key=str(org)+","+str(phone)
            if(phoneblack=="1"):
                val=str(time)+","+str(1)
            else:
                val=str(time)+","+str(0)
            print("\t".join([key,val]))

    except Exception as e:
        update_mr_counter('Mapper','Error' + str(e), 1)