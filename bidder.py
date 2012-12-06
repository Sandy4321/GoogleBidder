#Real Time Bidding Engine for Google Ad Exchange
#Sensitive code, lot of logics have been used at a lot of places. Changing might cause un-noticable bugs

import sys, traceback
import random
import time  
import hashlib
import re
import json
import datetime
import base64
import time
import operator
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.options
import tornadoredis
import tornado.gen
#import redis
import os
import sqlite3
import thread
import csv
import realtime_bidding_proto_pb2
from pytz import timezone
from pyDes import *
from urlparse import urlparse
from tornado.web import asynchronous
from collections import defaultdict
from tornado.options import define, options

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self):
	global geoIndex
	global campaignData
	global ruleSet
	global con
	global cur
	global india_tz
	global india_time
        start = time.time()
        postContent = self.request.body
        bidRequest = realtime_bidding_proto_pb2.BidRequest()
        try:
	    bidRequest.ParseFromString(postContent)
            domain = re.sub('www.',r'',str(urlparse(bidRequest.url).netloc))

            geo_criteria_id=bidRequest.geo_criteria_id
	    if str(geo_criteria_id) in geoIndex.keys():
		country = geoIndex[str(geo_criteria_id)]["Country"].lower()
		cType= geoIndex[str(geo_criteria_id)]["Type"]
		if cType=="City":
		    city=geoIndex[str(geo_criteria_id)]["Name"].lower()
		    state=geoIndex[geoIndex[str(geo_criteria_id)]["Parent"]]["Name"].lower()
		if cType=="State":
		    state=geoIndex[str(geo_criteria_id)]["Name"].lower()
		    city=""
		if cType=="UT":
		    state=geoIndex[str(geo_criteria_id)]["Name"].lower()
		    city=""

	    if not bidRequest.HasField("anonymous_id") and not bidRequest.HasField("mobile") and not bidRequest.HasField("video") and not bidRequest.is_ping:
		#segments = yield tornado.gen.Task(redisClient.smembers,'user:'+bidRequest.google_user_id)
		segments=[]
		adSlots = bidRequest.adslot
		response = realtime_bidding_proto_pb2.BidResponse()

		for ad in adSlots:
		    if segments:
			for seg in segments:
			    try:
				audienceCampaigns = campaignData['display:audience:' + str(seg)]
			    except KeyError:
				audienceCampaigns=list()
		    else:
			audienceCampaigns=list()

		    try:
			ronCampaigns = campaignData['display:roe']
		    except KeyError:
			ronCampaigns = list()

		    try:
			black = campaignData['display:roe:black:'+domain]		#ROE Campaigns with this domain as blacklist
		    except KeyError:
			black = list()

		    ronCampaigns = list(set(ronCampaigns) - set(black))			

		    try:
		        whiteCampaigns = campaignData['display:white:'+domain]
		    except KeyError:
			whiteCampaigns = list()

		    campaigns = list(set(audienceCampaigns+ronCampaigns+whiteCampaigns))
		    
		    geoCampaigns = campaignData['display:geo:'+country]
		    campaigns = list(set(geoCampaigns) & set(campaigns))
		    
		    size=str(ad.width[0])+"x"+str(ad.height[0])
		    try:
		        sizeCampaigns = campaignData['display:size:'+size]
		    except KeyError:
			sizeCampaigns = list()
			
		    campaigns = list(set(sizeCampaigns) & set(campaigns))
		    
		    if(len(campaigns)>0):
			camplist=[]
			for camp in campaigns:
			    l = [camp, campaignData["display:campaign:"+str(camp)+":bid"],campaignData["display:campaign:"+str(camp)+":pacing"]]
			    camplist.append(l)
			camplist.sort(key=operator.itemgetter(1), reverse=True) # sorts the list in place decending by bids

			#Retrieve rules from SQLLite and create rule dictionary
			ruleDict=dict()
			hour=india_time.strftime('%H')
			if hour>=2 and hour<6:
			  daypart=1
			if hour>=6 and hour<10:
			  daypart=2
			if hour>=10 and hour<14:
			  daypart=3
			if hour>=14 and hour<18:
			  daypart=4
			if hour>=18 and hour<22:
			  daypart=5
			if hour>=22 and hour<2:
			  daypart=6
			weekday=india_time.strftime('%w')
			if city=='':
			  query = "SELECT * FROM rules WHERE (domain='"+domain+"' OR domain IS NULL) AND (city='"+city+"' OR city IS NULL) AND (state='"+state+"' OR state IS NULL) AND (weekday='"+weekday+"' OR weekday IS NULL) AND (hour='"+hour+"' OR hour IS NULL) AND (daypart='"+daypart+"' OR daypart IS NULL) AND (size='"+size+"' OR size IS NULL) AND (isp='"+isp+"' OR isp IS NULL) ORDER BY dimensions ASC"
			else:
			  query = "SELECT * FROM rules WHERE (domain='"+domain+"' OR domain IS NULL) AND city IS NULL AND (state='"+state+"' OR state IS NULL) AND (weekday='"+weekday+"' OR weekday IS NULL) AND (hour='"+hour+"' OR hour IS NULL) AND (daypart='"+daypart+"' OR daypart IS NULL) AND (size='"+size+"' OR size IS NULL) AND (isp='"+isp+"' OR isp IS NULL) ORDER BY dimensions ASC"
			cur.execute(query)
			rows=cur.fetchall()
			for row in rows:
			  rules=json.loads(row[9])
			  for key in rules.keys():
			    ruleDict[key]=float(rules[key])

			#Loop over qualified campaigns and override the default bids with new bids from rules database
			newCampList=[]
			for camp in camplist:
			  if str(camp[0]) in rulesDict.keys():
			    camp[1]=float(rulesDict[str(camp[0])])
			  newCampList.append(camp)
			  
			#Now start qualifying campaigns top-down by bids for pacing. If a campaign qualifies, choose it as a final candidate
			finalCampaign=0
			for camp in newCampList:
			  r=random.randrange(1,100)
			  if r<camp[2]:
			    finalCampaign=camp[0]
			    finalBid=camp[1]
			    break
			    
			if finalCampaign>0:    
			    banners = campaignData['display:campaign:'+str(finalCampaign)+':'+str(ad.width[0])+'x'+str(ad.height[0])]
			    randomBannerId = random.choice(banners)
			    bidMicros = finalBid * 1000000
			    info = base64.b64encode(json.dumps({'e':'GOOGLE','d':domain,'bid':randomBannerId,'cid':finalCampaign}))
			    info = info.replace("+","-").replace("/","_").replace("=","")
			    code='<iframe src="http://rtbidder.impulse01.com/serve?info='+info+'&p={WINNING_PRICE}&r={RANDOM}&red={CLICKURL}" width="'+str(ad.width[0])+'" height="'+str(ad.height[0])+'" frameborder=0 marginwidth=0 marginheight=0 scrolling=NO></iframe>'
			    responsead = response.ad.add()
			    responsead.html_snippet = code
			    responsead.creative_id= randomBannerId
			    responsead.click_through_url.append(campaignData['display:campaign:'+str(finalCampaign)+':url'])
			    responseAdSlot = responsead.adslot.add()
			    responseAdSlot.id=ad.id
			    responseAdSlot.max_cpm_micros=int(bidMicros)
		response.processing_time_ms=int((time.time()-start)*1000)
	    else:
		response = realtime_bidding_proto_pb2.BidResponse()
		response.processing_time_ms=int((time.time()-start)*1000)
	except:
	    response = realtime_bidding_proto_pb2.BidResponse()
	    response.processing_time_ms=int((time.time()-start)*1000)
	    traceback.print_exc(file=sys.stdout)

        responseString = response.SerializeToString()
	self.write(responseString)
	self.finish()
	

	
	
#---------------------Refresh Campaign Index------------------------------------------------
def refreshCache():
    global campaignData
    try:
	http_client = httpclient.AsyncHTTPClient()
	response=yield gen.Task(http_client.fetch, "http://user.impulse01.com:5003/index?channel=1")
        invertedIndex=json.loads(response.body)
    except:
        invertedIndex=dict()
    campaignData=invertedIndex
    print options.name+" Refreshed campaign inverted index from http://user.impulse01.com:5003/index?channel=1"
    del invertedIndex
#-----------------------------------------------------------------------------------------------





#---------------------Refresh Rules Database------------------------------------------------
@tornado.web.asynchronous
def refreshRules():
    http_client = httpclient.AsyncHTTPClient()
    http_client.fetch("http://user.impulse01.com:5003/rules?channel=1", handleRulesFetch)
    print options.name+" is fetching new rules from http://user.impulse01.com:5003/rules?channel=1"    
    
def handleRulesFetch(response):    
    global con
    global cur
    try:
	rulesIndex=json.loads(response.body)
    except:
	rulesIndex=dict()
    queryData=[]
    for key in rulesIndex.keys():
	sm=key.split("|")
	for n,i in enumerate(sm):
	  if i=='*':
	    sm[n]=None
	record = (sm[0],sm[1],sm[2],sm[3],sm[4],sm[5],sm[6],sm[7],(8-key.count("*")),json.dumps(rulesIndex[key]))
	queryData.append(record)
    cur.execute("DELETE FROM rules")
    cur.executemany('INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?,?)', queryData)    
    print "inserted "+str(len(rulesIndex.keys()))+" records into rules table"
    print options.name+" Refreshed rules index from http://user.impulse01.com:5003/rules?channel=1"    
#-----------------------------------------------------------------------------------------------





#----------------------Initialize the Tornado Server --------------------------------
define("port", default=8888, help="run on the given port", type=int)
define("name", default="noname", help="name of the server")
define("refreshCache", default=10000, help="millisecond interval between cache refresh", type=int)
define("rulesRefresh", default=10000, help="millisecond interval between rules refresh", type=int)
#sredisClient = tornadoredis.Client('cookie-tokyo.impulse01.com')
#redisClient.connect()
application = tornado.web.Application([(r".*", MainHandler),])
#-----------------------------------------------------------------------------------------------



#---------------------Load Geo Index------------------------------------------------
geoIndex=dict()
location = open("location.csv","r").read()
reader = csv.reader(location.split('\n'), delimiter=',')
for row in reader:
  geoIndex[row[0]]={"Name":row[1], "Parent":row[3], "Type":row[5],"Country":row[4]}
print options.name+" Loaded geoIndex from location.csv"
del location
del reader
#-----------------------------------------------------------------------------------------------



#---------------------Construct Campaign Index------------------------------------------------
campaignData=dict()
http_client = tornado.httpclient.HTTPClient()
try:
    response = http_client.fetch("http://user.impulse01.com:5003/index?channel=1")
    invertedIndex=json.loads(response.body)
except:
    invertedIndex=dict()
campaignData=invertedIndex
print options.name+" Loaded campaign inverted index from http://user.impulse01.com:5003/index?channel=1"
del response
del invertedIndex
#-----------------------------------------------------------------------------------------------



#-----------------------Construct the Rules Database ---------------------------------------------
http_client = tornado.httpclient.HTTPClient()
try:
    response = http_client.fetch("http://user.impulse01.com:5003/rules?channel=1")
    rulesIndex=json.loads(response.body)
except:
    rulesIndex=dict()
print options.name+" Loaded rules index from http://user.impulse01.com:5003/rules?channel=1"
print "total "+str(len(rulesIndex.keys()))+" rules"
print "creating in-memory sqlite database"
con = sqlite3.connect(":memory:")
con.isolation_level = None
cur = con.cursor()
cur.execute('''CREATE TABLE rules (domain, city, state, weekday, hour, daypart,size,isp,dimensions,bids)''')
cur.execute('CREATE INDEX ind ON rules(domain, city, state, weekday, hour, daypart,size,isp)')
queryData=[]
for key in rulesIndex.keys():
    sm=key.split("|")
    for n,i in enumerate(sm):
      if i=='*':
        sm[n]=None
    record = (sm[0],sm[1],sm[2],sm[3],sm[4],sm[5],sm[6],sm[7],(8-key.count("*")),json.dumps(rulesIndex[key]))
    queryData.append(record)
cur.executemany('INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?,?)', queryData)    
print "inserted "+str(len(rulesIndex.keys()))+" records into rules table"
print "created index on SQLite table rules"
del rulesIndex
del queryData
del response
del sm
del http_client
#-----------------------------------------------------------------------------------------------



india_tz = timezone('Asia/Kolkata')
india_time = datetime.datetime.now(india_tz)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.PeriodicCallback(refreshRules, options.refreshCache).start()    
    tornado.ioloop.IOLoop.instance().start() 