#Real Time Bidding Engine for Google Ad Exchange
#Sensitive code, lot of logics have been used at different places. Changing might cause un-noticable bugs
#Copyright(C) - Impulse Media Private Limited
#Authored - Aditya Singh and Phaneendra Hegde

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
import socket
import csv
import realtime_bidding_proto_pb2
from pytz import timezone
from pyDes import *
from urlparse import urlparse
from tornado.web import asynchronous
from collections import defaultdict
from tornado.options import define, options

#Address of the forecasting server
UDP_IP = "46.137.241.79"
UDP_PORT = 5006

class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def post(self):
        if self.request.path == "/":
	    self.write("Nothing for u here. Go <a href='http://google.com'>here</a>")
      
        if self.request.path == "/getbid":
	    global geoIndex
	    global campaignData
	    global ruleSet
	    global con
	    global cur
	    global india_tz
	    global india_time
	    global bidCountIndex
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

		print "Debug: Got request for "+domain+" from city "+city+" , state "+state
		
		if not bidRequest.HasField("anonymous_id") and not bidRequest.HasField("mobile") and not bidRequest.HasField("video") and not bidRequest.is_ping:
		    #segments = yield tornado.gen.Task(redisClient.smembers,'user:'+bidRequest.google_user_id)
		    segments=[]
		    adSlots = bidRequest.adslot
		    response = realtime_bidding_proto_pb2.BidResponse()

		    for ad in adSlots:
		        print "Debug: Evaluating ad slot "+str(ad)
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

			print "Debug: Ron Campaigns"+str(ronCampaigns)
			try:
			    black = campaignData['display:roe:black:'+domain]		#ROE Campaigns with this domain as blacklist
			except KeyError:
			    black = list()
			print "Debug: Ron Blacklisted Campaigns"+str(black)
			
			ronCampaigns = list(set(ronCampaigns) - set(black))			
			
			print "Debug: Ron Campaigns after black filtering"+str(ronCampaigns)
			
			try:
			    whiteCampaigns = campaignData['display:white:'+domain]
			except KeyError:
			    whiteCampaigns = list()

			print "Debug: White Campaigns"+str(whiteCampaigns)

			campaigns = list(set(audienceCampaigns+ronCampaigns+whiteCampaigns))
			
			print "Debug: All Campaigns"+str(campaigns)			
			
			try:
			    geoCampaigns = campaignData['display:geo:'+country]
			except KeyError:
			    geoCampaigns = list()
			    
			campaigns = list(set(geoCampaigns) & set(campaigns))
			
			print "Debug: All Campaigns After geo filtering"+str(campaigns)
			
			size=str(ad.width[0])+"x"+str(ad.height[0])
			try:
			    sizeCampaigns = campaignData['display:size:'+size]
			except KeyError:
			    sizeCampaigns = list()
			    
			campaigns = list(set(sizeCampaigns) & set(campaigns))

			print "Debug: All Campaigns After size filtering"+str(campaigns)

			if(len(campaigns)>0):
			    camplist=[]
			    for camp in campaigns:
				l = [camp, campaignData["display:campaign:"+str(camp)+":bid"],campaignData["display:campaign:"+str(camp)+":pacing"]]
				camplist.append(l)
				
			    print "Debug: Campaign List"+str(camplist)		    

			    #Retrieve rules from SQLLite and create rule dictionary
			    ruleDict=dict()
			    hour=int(india_time.strftime('%H'))
			    print "Debug: hour="+str(hour)
			    if hour>=2 and hour<6:
				daypart="1"
			    if hour>=6 and hour<10:
				daypart="2"
			    if hour>=10 and hour<14:
				daypart="3"
			    if hour>=14 and hour<18:
				daypart="4"
			    if hour>=18 and hour<22:
				daypart="5"
			    if hour>=22 or hour<2:
				daypart="6"
			    hour=str(hour)
			    weekday=india_time.strftime('%w')
			    if city=='':
				query = "SELECT * FROM rules WHERE (domain='"+domain+"' OR domain IS NULL) AND (city='"+city+"' OR city IS NULL) AND (state='"+state+"' OR state IS NULL) AND (weekday='"+weekday+"' OR weekday IS NULL) AND (hour='"+hour+"' OR hour IS NULL) AND (daypart='"+daypart+"' OR daypart IS NULL) AND (size='"+size+"' OR size IS NULL) ORDER BY dimensions ASC"
			    else:
				query = "SELECT * FROM rules WHERE (domain='"+domain+"' OR domain IS NULL) AND city IS NULL AND (state='"+state+"' OR state IS NULL) AND (weekday='"+weekday+"' OR weekday IS NULL) AND (hour='"+hour+"' OR hour IS NULL) AND (daypart='"+daypart+"' OR daypart IS NULL) AND (size='"+size+"' OR size IS NULL) ORDER BY dimensions ASC"
			    cur.execute(query)
			    rows=cur.fetchall()
			    
			    print "Debug: Matching rules "+str(len(rows))
			    for row in rows:
				rules=json.loads(row[8])
				for key in rules.keys():
				    ruleDict[key]=float(rules[key])
				    
			    print "Debug: New bids after applying rules"+str(ruleDict)

			    #Loop over qualified campaigns and override the default bids with new bids from rules database
			    newCampList=[]
			    for camp in camplist:
				if str(camp[0]) in ruleDict.keys():
				    camp[1]=float(ruleDict[str(camp[0])])
				newCampList.append(camp)
				
			    print "Debug: Campaign bids after overriding rules"+str(newCampList)

			    newCampList.sort(key=operator.itemgetter(1), reverse=True) # sorts the list in place decending by bids
			    
			    print "Debug: Campaign List After Sorting by bids"+str(newCampList)	

			    #Now start qualifying campaigns top-down by bids for pacing. If a campaign qualifies, choose it as a final candidate
			    finalCampaign=0
			    for camp in newCampList:
				r=random.randrange(1,100)
				if r<camp[2]:
				    print "Debug: Campaign "+str(camp[0])+" Qualified for bidding"
				    finalCampaign=camp[0]
				    finalBid=camp[1]
				    break
				else:
				    print "Debug: Campaign "+str(camp[0])+" did not qualify"			    

			    response.debug_string=str(finalCampaign)
			    
			    if finalCampaign>0:
			        print "Debug: Campaign "+str(finalCampaign)+" proceeding to bid"
				banners = campaignData['display:campaign:'+str(finalCampaign)+':'+str(ad.width[0])+'x'+str(ad.height[0])]
				randomBannerId = random.choice(banners)
				print "Debug: Choosen creative"+str(randomBannerId)
				bidMicros = finalBid * 1000000
				info = base64.b64encode(json.dumps({'e':'google','impid':base64.b64encode(bidRequest.id),'d':domain,'bid':randomBannerId,'cid':finalCampaign, 'b':finalBid}))
				info = info.replace("+","-").replace("/","_").replace("=","")
				code='<iframe src="http://rtbidder.impulse01.com/serve?info='+info+'&p=%%WINNING_PRICE%%&r=%%CACHEBUSTER%%&red=%%CLICK_URL_UNESC%%" width="'+str(ad.width[0])+'" height="'+str(ad.height[0])+'" frameborder=0 marginwidth=0 marginheight=0 scrolling=NO></iframe>'
				responsead = response.ad.add()
				responsead.html_snippet = code
				responsead.buyer_creative_id= str(randomBannerId)
				responsead.advertiser_name.append(campaignData['display:campaign:'+str(finalCampaign)+':advertiserName'])
				responsead.attribute.append(2)
				responsead.category.append(0)
				responsead.click_through_url.append(campaignData['display:campaign:'+str(finalCampaign)+':url'])
				responseAdSlot = responsead.adslot.add()
				responseAdSlot.id=ad.id
				responseAdSlot.max_cpm_micros=int(bidMicros)
		    response.processing_time_ms=int((time.time()-start)*1000)
		else:
		    response = realtime_bidding_proto_pb2.BidResponse()
		    response.processing_time_ms=int((time.time()-start)*1000)
		    
		bidCountIndex["GoogleAdX"][domain]["DesktopDisplay"][country.upper()][str(ad.width[0])+'x'+str(ad.height[0])]["Impressions"] += 1
		if int(time.time() - bidCountIndex["GoogleAdX"][domain]["DesktopDisplay"][country.upper()][str(ad.width[0])+'x'+str(ad.height[0])]["Lastupdate"])>10:
		  i = bidCountIndex["GoogleAdX"][domain]["DesktopDisplay"][country.upper()][str(ad.width[0])+'x'+str(ad.height[0])]["Impressions"]
		  message = json.dumps({"messageType":"Forecast", "message":{"e":"GoogleAdX", "d":domain , "c":"DesktopDisplay" ,"geo":country.upper(),
					  "size":str(ad.width[0])+'x'+str(ad.height[0]) , "i":i}})
		  bidCountIndex["GoogleAdX"][domain]["DesktopDisplay"][country.upper()][str(ad.width[0])+'x'+str(ad.height[0])]["Impressions"]=0
		  bidCountIndex["GoogleAdX"][domain]["DesktopDisplay"][country.upper()][str(ad.width[0])+'x'+str(ad.height[0])]["Lastupdate"]=time.time()
		  sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		  sock.sendto(message, (UDP_IP, UDP_PORT))
		
	    except:
		response = realtime_bidding_proto_pb2.BidResponse()
		response.processing_time_ms=int((time.time()-start)*1000)
		traceback.print_exc(file=sys.stdout)

	    print response
	    responseString = response.SerializeToString()
	    self.write(responseString)
	    self.finish()
    
	if self.request.path == "/result":
	    self.write("Nop")
	    
def autovivify(levels=1, final=dict):
    return (defaultdict(final) if levels < 2 else defaultdict(lambda: autovivify(levels - 1, final)))

	
#---------------------Refresh Campaign Index------------------------------------------------
def refreshCache():
    global campaignData
    http_client = tornado.httpclient.HTTPClient()
    try:
	response = http_client.fetch("http://user.impulse01.com:5003/index?channel=1")
	invertedIndex=json.loads(response.body)
    except:
        invertedIndex=dict()
    campaignData=invertedIndex
    print options.name+" Refreshed campaign inverted index from http://user.impulse01.com:5003/index?channel=1"
    del invertedIndex
#-----------------------------------------------------------------------------------------------





#---------------------Refresh Rules Database------------------------------------------------
def refreshRules():
    global con
    global cur  
    http_client = tornado.httpclient.HTTPClient()
    try:
	response = http_client.fetch("http://user.impulse01.com:5003/rules?channel=1")
	rulesIndex=json.loads(response.body)
    except:
	rulesIndex=dict()
    print options.name+" is fetching new rules from http://user.impulse01.com:5003/rules?channel=1"    
    queryData=[]
    for key in rulesIndex.keys():
	sm=key.split("|")
	for n,i in enumerate(sm):
	    if i=='*':
		sm[n]=None
	record = (sm[0],sm[1],sm[2],sm[3],sm[4],sm[5],sm[6],(8-key.count("*")),json.dumps(rulesIndex[key]))
	queryData.append(record)
    cur.execute("DELETE FROM rules")
    cur.executemany('INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?)', queryData)    
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
cur.execute('''CREATE TABLE rules (domain, city, state, weekday, hour, daypart,size,dimensions,bids)''')
cur.execute('CREATE INDEX ind ON rules(domain, city, state, weekday, hour, daypart,size)')
queryData=[]
for key in rulesIndex.keys():
    sm=key.split("|")
    for n,i in enumerate(sm):
	if i=='*':
	    sm[n]=None
    record = (sm[0],sm[1],sm[2],sm[3],sm[4],sm[5],sm[6],(8-key.count("*")),json.dumps(rulesIndex[key]))
    queryData.append(record)
cur.executemany('INSERT INTO rules VALUES (?,?,?,?,?,?,?,?,?)', queryData)    
print "inserted "+str(len(rulesIndex.keys()))+" records into rules table"
print "created index on SQLite table rules"
del rulesIndex
del queryData
del response
del sm
del http_client
#-----------------------------------------------------------------------------------------------

bidCountIndex = autovivify(6, int)
india_tz = timezone('Asia/Kolkata')
india_time = datetime.datetime.now(india_tz)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.PeriodicCallback(refreshRules, options.refreshCache).start()
    tornado.ioloop.IOLoop.instance().start()     