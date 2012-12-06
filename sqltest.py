import time
import json
from collections import defaultdict
import sqlite3
import timeit

con = sqlite3.connect(":memory:")
con.isolation_level = None
cur = con.cursor()
cur.execute('''CREATE TABLE rules
             (domain, city, state, weekday, hour, daypart,size,isp,dimensions)''')
cur.execute('CREATE INDEX ind ON rules(domain, city, state, weekday, hour, daypart,size,isp)')

rulesData = open("rules30k.json","r").read()
rulesIndex=json.loads(rulesData)
queryData=[]
for key in rulesIndex.keys():
    value=rulesIndex[key]
    dimensions=str(8-key.count("*"))
    sm=key.split("|")
    for n,i in enumerate(sm):
      if i=='*':
        sm[n]=None
    record = (sm[0],sm[1],sm[2],sm[3],sm[4],sm[5],sm[6],sm[7],dimensions)
    queryData.append(record)
    
cur.executemany('INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?)', queryData)    
print "inserted "+str(len(rulesIndex.keys()))+" records"

domain = "baltimoresun.com"
city = "kakinada"
state = "maharashtra"
weekday ="5" 
hour="16"
daypart="6"
size="1"
isp = "4"

start=time.time()
query = "SELECT * FROM rules WHERE (domain='"+domain+"' OR domain IS NULL) AND (city='"+city+"' OR city IS NULL) AND (state='"+state+"' OR state IS NULL) AND (weekday='"+weekday+"' OR weekday IS NULL) AND (hour='"+hour+"' OR hour IS NULL) AND (daypart='"+daypart+"' OR daypart IS NULL) AND (size='"+size+"' OR size IS NULL) AND (isp='"+isp+"' OR isp IS NULL)"
cur.execute(query)
#cur.execute("SELECT * FROM rules WHERE (domain='"+domain+"' OR domain='*') AND (city='"+city+"' OR city='*') AND (state='"+state+"' OR state ='*') AND (weekday='"+weekday+"' OR weekday='*') AND (hour='"+hour+"' OR hour='*') AND (daypart='"+daypart+"' OR daypart='*') AND (size='"+size+"' OR size='*') AND (isp='"+isp+"' OR isp='*')")
timeTaken=time.time() - start
print timeTaken*1000