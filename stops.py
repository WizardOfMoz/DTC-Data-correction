def find_stop_reached_in_route(v_lat, v_lng, stops_lats,stops_lngs):
    

    stst = 6367 * 2 * np.arcsin(np.sqrt(
        np.sin((np.radians(stops_lats) - math.radians(v_lat)) / 2) ** 2 + math.cos(
            math.radians(v_lat)) * np.cos(np.radians(stops_lats)) * np.sin(
            (np.radians(stops_lngs) - math.radians(v_lng)) / 2) ** 2))
    return np.where(stst <= 0.3)[0].tolist()



def update(s):                          #for string matching
    s=s.upper().replace(" ","")
    s=rem(s)
    return s
def rem(s):
    n=s.find("UP")
    if n==-1:
        n=s.find("DOWN")
    if n!=-1:
        return s[:n]
    else:
        return s

def append(s):
    s=s+"_06_00"
    return s

import math
import numpy as np
import Levenshtein as lev
import pandas as pd
import sqlite3 as sql
import time
import pickle
from geopy.distance import geodesic 
start=time.time()
con=sql.connect("data/bus_movements_2020_12_14.db")  
routes=pd.read_csv("updated_stops/routes.txt")
times=pd.read_csv("updated_stops/stop_times.txt")
stops=pd.read_csv("updated_stops/stops.txt")

times=times.groupby("trip_id")

realtime=pd.read_csv("fleet_charts_dec_01_14/14_12_2020.csv")

d={}
map={}
static_loc={}
right=set({})
wrong=dict()
try:
    db=open("right","rb")
    right=set(pickle.load(db))
    db.close()
    db=open("wrong","rb")
    wrong=dict(pickle.load(db))
    db.close()
except:
    print("start")

for i in range(len(stops)):
    id=stops.at[i,"stop_id"]
    lat=stops.at[i,"stop_lat"]
    lon=stops.at[i,"stop_lon"]
    map[id]=(lat,lon)

    
for i in range(len(realtime)):
    s=realtime.at[i,"duty"]
    vehicle=realtime.at[i,"bus_number"]
    ratio=0
    id=-1
    for j in range(len(routes)):
        s1=routes.at[j,"route_long_name"]
        f=lev.ratio(update(s),update(s1))
        if f>ratio:
            id=routes.at[j,"route_id"]
            ratio=f
    
    if(id!=-1 and ratio>0.95):
        d[vehicle]=id


print(time.time()-start)
start=time.time()

for vehicle in d:
    route=d[vehicle]
    id=append(str(route))
    group=times.get_group(id)
    static_loc[vehicle]=[]
    for i in range(len(group)):
        stop=group.iloc[i].stop_id
        static_loc[vehicle].append(stop)
print(time.time()-start)

mess=[]
ctr=0
total=0
damn=0
start=time.time()
print()
print(len(d))

for vehicle in d:
    route=d[vehicle]
    static=static_loc[vehicle]
    query=f"SELECT lat,lng FROM foo WHERE vehicle_id='{vehicle}'"
    df=pd.read_sql_query(query,con)
    stops_lats=df['lat'].values.astype(np.float)
    stops_lngs=df['lng'].values.astype(np.float)
    ctr+=1
    if ctr%1000==0:
        print("processing")
    for i in static:
        a=map[i]
        if len(df)!=0:
            v=find_stop_reached_in_route(a[0],a[1],stops_lats,stops_lngs)
            if len(v)!=0:
                right.add(i)
            else:
                if i not in wrong.keys():
                    wrong[i]=set({})
                    wrong[i].add(route)
                else:
                    wrong[i].add(route)
temp=set(wrong.keys())
temp=temp-right
new_dict = {key:val for key, val in wrong.items() if key in temp}
wrong=new_dict
print(len(wrong))

db=open("wrong","wb")
pickle.dump(wrong,db)
db.close()

db=open("right","wb")
pickle.dump(right,db)
db.close()

con.close()

        






