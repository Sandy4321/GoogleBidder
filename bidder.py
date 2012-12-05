import sys
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
        start = time.time()
        postContent = self.request.body
        bidRequest = realtime_bidding_proto_pb2.BidRequest()
        print bidRequest
        try:
	    bidRequest.ParseFromString(postContent)
            domain = re.sub('www.',r'',str(urlparse(bidRequest.url).netloc))

            geo_criteria_id=bidRequest.geo_criteria_id
	    if str(geo_criteria_id) in geoIndex:
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

	    if not bidRequest.HasField("anonymous_id") and not bidRequest.HasField("Mobile") and not bidRequest.HasField("Video") and not bidRequest.is_ping:
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

		    try:
			ronCampaigns = campaignData['display:roe']			#Get all ROE Campaigns
			black = campaignData['display:roe:black:'+domain]		#ROE Campaigns with this domain as blacklist
			ronCampaigns = list(set(ronCampaigns) - set(black))
		    except KeyError:
			ronCampaigns = list()

		    try:
		        whiteCampaigns = campaignData['display:white:'+domain]
		    except KeyError:
			whiteCampaigns = list()

		    campaigns = list(set(audienceCampaigns+ronCampaigns+whiteCampaigns))

		    geoCampaigns = campaignData['display:geo:'+country]
		    campaigns = list(set(geoCampaigns) & set(campaigns))
		    
		    size=str(ad.width)+"x"+str(ad.height)
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

			finalCampaign=0
			for camp in camplist:
			  r=random.randrange(1,100)
			  if r<camp[2]:
			    finalCampaign=camp[0]
			    break

			if finalCampaign>0:    
			    finalBid = campaignData["display:campaign:"+str(camp)+":bid"]
			    banners = campaignData['display:campaign:'+str(finalCampaign)+':'+width+'x'+height]
			    randomBannerId = random.choice(banners)
			    finalResult = {'campaignId':finalCampaign,'bannerId':randomBannerId,'bid':finalBid}
			    bidMicros = bid['bid'] * 1000000
			    info = base64.b64encode(json.dumps({'e':'GOOGLE','d':domain,'bid':bid['bannerId'],'cid':bid['campaignId']}))
			    info = info.replace("+","-").replace("/","_").replace("=","")
			    code='<iframe src="http://rtbidder.impulse01.com/serve?info='+info+'&p={WINNING_PRICE}&r={RANDOM}&red={CLICKURL}" width="'+str(ad.width)+'" height="'+str(ad.height)+'" frameborder=0 marginwidth=0 marginheight=0 scrolling=NO></iframe>'   
			    responsead = response.ad.add()
			    responsead.html_snippet = code
			    responsead.creative_id= randomBannerId
			    responsead.click_through_url=campaignData['display:campaign:'+str(finalCampaign)+':url']
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

        responseString = response.SerializeToString()
	self.write(responseString)
	self.finish()
	
def autovivify(levels=1, final=dict):
    return (defaultdict(final) if levels < 2 else defaultdict(lambda: autovivify(levels - 1, final)))
    
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

def refreshRules():
    global rulesIndex
    rulesTemp = autovivify(8, int)
    http_client = tornado.httpclient.HTTPClient()
    try:
	response = http_client.fetch("http://user.impulse01.com:5003/rules?channel=1")
	invertedIndex=json.loads(response.body)
    except:
	invertedIndex=dict()
    for key in invertedIndex.keys():
	value=invertedIndex[key]
	sm=key.split("|")
	rulesTemp[sm[0]][sm[1]][sm[2]][sm[3]][sm[4]][sm[5]][sm[6]][sm[7]]=value
    rulesIndex=rulesTemp
    print options.name+" Refreshed rules index from http://user.impulse01.com:5003/rules?channel=1"    

define("port", default=8888, help="run on the given port", type=int)
define("name", default="noname", help="name of the server")
define("refreshCache", default=10000, help="millisecond interval between cache refresh", type=int)
define("rulesRefresh", default=10000, help="millisecond interval between rules refresh", type=int)
#sredisClient = tornadoredis.Client('cookie-tokyo.impulse01.com')
#redisClient.connect()
application = tornado.web.Application([(r".*", MainHandler),])



#---------------------Load Geo Index------------------------------------------------
geoIndex=dict()
location = open("location.csv","r").read()
reader = csv.reader(location.split('\n'), delimiter=',')
for row in reader:
  geoIndex[row[0]]={"Name":row[1], "Parent":row[3], "Type":row[5]}
print options.name+" Loaded geoIndex from location.csv"
#-----------------------------------------------------------------------------------------------



#---------------------Construct Inverted Index------------------------------------------------
campaignData=dict()
http_client = tornado.httpclient.HTTPClient()
try:
    response = http_client.fetch("http://user.impulse01.com:5003/index?channel=1")
    invertedIndex=json.loads(response.body)
except:
    invertedIndex=dict()
campaignData=invertedIndex
print options.name+" Loaded campaign inverted index from http://user.impulse01.com:5003/index?channel=1"
#-----------------------------------------------------------------------------------------------



#-----------------------Construct the Rules Index ---------------------------------------------
rulesIndex = autovivify(8, int)
http_client = tornado.httpclient.HTTPClient()
try:
    response = http_client.fetch("http://user.impulse01.com:5003/rules?channel=1")
    invertedIndex=json.loads(response.body)
except:
    invertedIndex=dict()
for key in invertedIndex.keys():
    value=invertedIndex[key]
    sm=key.split("|")
    rulesIndex[sm[0]][sm[1]][sm[2]][sm[3]][sm[4]][sm[5]][sm[6]][sm[7]]=value
print options.name+" Loaded rules index from http://user.impulse01.com:5003/rules?channel=1"    
#-----------------------------------------------------------------------------------------------

ruleSet = "1|*|*|*|*|*|*|*,*|2|*|*|*|*|*|*,*|*|3|*|*|*|*|*,*|*|*|4|*|*|*|*,*|*|*|*|5|*|*|*,*|*|*|*|*|6|*|*,*|*|*|*|*|*|7|*,*|*|*|*|*|*|*|8,1|2|*|*|*|*|*|*,1|*|3|*|*|*|*|*,1|*|*|4|*|*|*|*,1|*|*|*|5|*|*|*,1|*|*|*|*|6|*|*,1|*|*|*|*|*|7|*,1|*|*|*|*|*|*|8,*|2|3|*|*|*|*|*,*|2|*|4|*|*|*|*,*|2|*|*|5|*|*|*,*|2|*|*|*|6|*|*,*|2|*|*|*|*|7|*,*|2|*|*|*|*|*|8,*|*|3|4|*|*|*|*,*|*|3|*|5|*|*|*,*|*|3|*|*|6|*|*,*|*|3|*|*|*|7|*,*|*|3|*|*|*|*|8,*|*|*|4|5|*|*|*,*|*|*|4|*|6|*|*,*|*|*|4|*|*|7|*,*|*|*|4|*|*|*|8,*|*|*|*|5|6|*|*,*|*|*|*|5|*|7|*,*|*|*|*|5|*|*|8,*|*|*|*|*|6|7|*,*|*|*|*|*|6|*|8,*|*|*|*|*|*|7|8,1|2|3|*|*|*|*|*,1|2|*|4|*|*|*|*,1|2|*|*|5|*|*|*,1|2|*|*|*|6|*|*,1|2|*|*|*|*|7|*,1|2|*|*|*|*|*|8,1|*|3|4|*|*|*|*,1|*|3|*|5|*|*|*,1|*|3|*|*|6|*|*,1|*|3|*|*|*|7|*,1|*|3|*|*|*|*|8,1|*|*|4|5|*|*|*,1|*|*|4|*|6|*|*,1|*|*|4|*|*|7|*,1|*|*|4|*|*|*|8,1|*|*|*|5|6|*|*,1|*|*|*|5|*|7|*,1|*|*|*|5|*|*|8,1|*|*|*|*|6|7|*,1|*|*|*|*|6|*|8,1|*|*|*|*|*|7|8,*|2|3|4|*|*|*|*,*|2|3|*|5|*|*|*,*|2|3|*|*|6|*|*,*|2|3|*|*|*|7|*,*|2|3|*|*|*|*|8,*|2|*|4|5|*|*|*,*|2|*
|4|*|6|*|*,*|2|*|4|*|*|7|*,*|2|*|4|*|*|*|8,*|2|*|*|5|6|*|*,*|2|*|*|5|*|7|*,*|2|*|*|5|*|*|8,*|2|*|*|*|6|7|*,*|2|*|*|*|6|*|8,*|2|*|*|*|*|7|8,*|*|3|4|5|*|*|*,*|*|3|4|*|6|*|*,*|*|3|4|*|*|7|*,*|*|3|4|*|*|*|8,*|*|3|*|5|6|*|*,*|*|3|*|5|*|7|*,*|*|3|*|5|*|*|8,*|*|3|*|*|6|7|*,*|*|3|*|*|6|*|8,*|*|3|*|*|*|7|8,*|*|*|4|5|6|*|*,*|*|*|4|5|*|7|*,*|*|*|4|5|*|*|8,*|*|*|4|*|6|7|*,*|*|*|4|*|6|*|8,*|*|*|4|*|*|7|8,*|*|*|*|5|6|7|*,*|*|*|*|5|6|*|8,*|*|*|*|5|*|7|8,*|*|*|*|*|6|7|8,1|2|3|4|*|*|*|*,1|2|3|*|5|*|*|*,1|2|3|*|*|6|*|*,1|2|3|*|*|*|7|*,1|2|3|*|*|*|*|8,1|2|*|4|5|*|*|*,1|2|*|4|*|6|*|*,1|2|*|4|*|*|7|*,1|2|*|4|*|*|*|8,1|2|*|*|5|6|*|*,1|2|*|*|5|*|7|*,1|2|*|*|5|*|*|8,1|2|*|*|*|6|7|*,1|2|*|*|*|6|*|8,1|2|*|*|*|*|7|8,1|*|3|4|5|*|*|*,1|*|3|4|*|6|*|*,1|*|3|4|*|*|7|*,1|*|3|4|*|*|*|8,1|*|3|*|5|6|*|*,1|*|3|*|5|*|7|*,1|*|3|*|5|*|*|8,1|*|3|*|*|6|7|*,1|*|3|*|*|6|*|8,1|*|3|*|*|*|7|8,1|*|*|4|5|6|*|*,1|*|*|4|5|*|7|*,1|*|*|4|5|*|*|8,1|*|*|4|*|6|7|*,1|*|*|4|*|6|*|8,1|*|*|4|*|*|7|8,1|*|*|*|5|6|7|*,1|*|*|*|5|6|*|8,1|*|*|*|5|*|7|8,1|*|*|*|*|6|7|8,*
|2|3|4|5|*|*|*,*|2|3|4|*|6|*|*,*|2|3|4|*|*|7|*,*|2|3|4|*|*|*|8,*|2|3|*|5|6|*|*,*|2|3|*|5|*|7|*,*|2|3|*|5|*|*|8,*|2|3|*|*|6|7|*,*|2|3|*|*|6|*|8,*|2|3|*|*|*|7|8,*|2|*|4|5|6|*|*,*|2|*|4|5|*|7|*,*|2|*|4|5|*|*|8,*|2|*|4|*|6|7|*,*|2|*|4|*|6|*|8,*|2|*|4|*|*|7|8,*|2|*|*|5|6|7|*,*|2|*|*|5|6|*|8,*|2|*|*|5|*|7|8,*|2|*|*|*|6|7|8,*|*|3|4|5|6|*|*,*|*|3|4|5|*|7|*,*|*|3|4|5|*|*|8,*|*|3|4|*|6|7|*,*|*|3|4|*|6|*|8,*|*|3|4|*|*|7|8,*|*|3|*|5|6|7|*,*|*|3|*|5|6|*|8,*|*|3|*|5|*|7|8,*|*|3|*|*|6|7|8,*|*|*|4|5|6|7|*,*|*|*|4|5|6|*|8,*|*|*|4|5|*|7|8,*|*|*|4|*|6|7|8,*|*|*|*|5|6|7|8,1|2|3|4|5|*|*|*,1|2|3|4|*|6|*|*,1|2|3|4|*|*|7|*,1|2|3|4|*|*|*|8,1|2|3|*|5|6|*|*,1|2|3|*|5|*|7|*,1|2|3|*|5|*|*|8,1|2|3|*|*|6|7|*,1|2|3|*|*|6|*|8,1|2|3|*|*|*|7|8,1|2|*|4|5|6|*|*,1|2|*|4|5|*|7|*,1|2|*|4|5|*|*|8,1|2|*|4|*|6|7|*,1|2|*|4|*|6|*|8,1|2|*|4|*|*|7|8,1|2|*|*|5|6|7|*,1|2|*|*|5|6|*|8,1|2|*|*|5|*|7|8,1|2|*|*|*|6|7|8,1|*|3|4|5|6|*|*,1|*|3|4|5|*|7|*,1|*|3|4|5|*|*|8,1|*|3|4|*|6|7|*,1|*|3|4|*|6|*|8,1|*|3|4|*|*|7|8,1|*|3|*|5|6|7|*,1|*|3|*|5|6|*|8,1|*|3|*|5|*|7|8,
1|*|3|*|*|6|7|8,1|*|*|4|5|6|7|*,1|*|*|4|5|6|*|8,1|*|*|4|5|*|7|8,1|*|*|4|*|6|7|8,1|*|*|*|5|6|7|8,*|2|3|4|5|6|*|*,*|2|3|4|5|*|7|*,*|2|3|4|5|*|*|8,*|2|3|4|*|6|7|*,*|2|3|4|*|6|*|8,*|2|3|4|*|*|7|8,*|2|3|*|5|6|7|*,*|2|3|*|5|6|*|8,*|2|3|*|5|*|7|8,*|2|3|*|*|6|7|8,*|2|*|4|5|6|7|*,*|2|*|4|5|6|*|8,*|2|*|4|5|*|7|8,*|2|*|4|*|6|7|8,*|2|*|*|5|6|7|8,*|*|3|4|5|6|7|*,*|*|3|4|5|6|*|8,*|*|3|4|5|*|7|8,*|*|3|4|*|6|7|8,*|*|3|*|5|6|7|8,*|*|*|4|5|6|7|8,1|2|3|4|5|6|*|*,1|2|3|4|5|*|7|*,1|2|3|4|5|*|*|8,1|2|3|4|*|6|7|*,1|2|3|4|*|6|*|8,1|2|3|4|*|*|7|8,1|2|3|*|5|6|7|*,1|2|3|*|5|6|*|8,1|2|3|*|5|*|7|8,1|2|3|*|*|6|7|8,1|2|*|4|5|6|7|*,1|2|*|4|5|6|*|8,1|2|*|4|5|*|7|8,1|2|*|4|*|6|7|8,1|2|*|*|5|6|7|8,1|*|3|4|5|6|7|*,1|*|3|4|5|6|*|8,1|*|3|4|5|*|7|8,1|*|3|4|*|6|7|8,1|*|3|*|5|6|7|8,1|*|*|4|5|6|7|8,*|2|3|4|5|6|7|*,*|2|3|4|5|6|*|8,*|2|3|4|5|*|7|8,*|2|3|4|*|6|7|8,*|2|3|*|5|6|7|8,*|2|*|4|5|6|7|8,*|*|3|4|5|6|7|8,1|2|3|4|5|6|7|*,1|2|3|4|5|6|*|8,1|2|3|4|5|*|7|8,1|2|3|4|*|6|7|8,1|2|3|*|5|6|7|8,1|2|*|4|5|6|7|8,1|*|3|4|5|6|7|8,*|2|3|4|5|6|7|8,1|2|3|4|5|6|7|8"

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.PeriodicCallback(refreshRules, options.refreshCache).start()    
    tornado.ioloop.IOLoop.instance().start() 
