import realtime_bidding_proto_pb2
import httplib, urllib
import random
import csv

#url = ["http://zyffg.com/map.php?id=33&jjhd=4456","http://moneycontrol.com","http://yahoo.com/hell.php","http://tornadoweb.com"]

geoIndex=dict()
location = open("location.csv","r").read()
reader = csv.reader(location.split('\n'), delimiter=',')
for row in reader:
    geoIndex[row[0]]={"Name":row[1], "Parent":row[3], "Type":row[5],"Country":row[4]}

#urllist=["http://investmentyogi.com","http://ndtv.com" ,"http://sulekha.com","http://amfimutualfund.com","http://apnapaisa.com","http://sify.com","http://holidayiq.com","http://indiatimes.com","http://sharetipsinfo.com","http://myzamana.com","http://ehow.com","http://mutualfundsnavindia.com","http://firstpost.com","http://appuonline.com"]
urllist=["http://bankingawareness.com","http://bankifsccode.com","http://lawyersclubindia.com","http://simpletaxindia.net","http://moneycontrol.com","http://onemint.com","http://apnapaisa.com"]
geo_criteria_id=[1007751,1007753,1007765,1007772,1007788,1007805,1007809,1007809,9040183]
size=[[300,250],[160,600],[728,90],[120,600]]
allbids="Url, Width, Height, GeoCountry, GeoState, GeoCity, Campaign, Bid, Creative, Advertiser"

conn = httplib.HTTPConnection("bid-hk.impulse01.com",80)
for i in range(100):
  request = realtime_bidding_proto_pb2.BidRequest()
  request.id="1112"
  request.is_ping = 0
  request.google_user_id="aditya123"
  geo = random.choice(geo_criteria_id)
  request.geo_criteria_id=geo
  url = random.choice(urllist)
  request.url=url
  ads = request.adslot.add()
  ads.id=33
  b=random.choice(size)
  ads.width.append(b[0])
  ads.height.append(b[1])
  requestString = request.SerializeToString()
  headers = {"Content-type": "application/octet-stream","Accept": "application/octet-stream"}
  conn.request("POST", "/getbid", requestString,headers)
  response = conn.getresponse()
  data = response.read()
  response = realtime_bidding_proto_pb2.BidResponse()
  response.ParseFromString(data)

  if str(geo) in geoIndex.keys():
      country = geoIndex[str(geo)]["Country"].lower()
      cType= geoIndex[str(geo)]["Type"]
      if cType=="City":
	  city=geoIndex[str(geo)]["Name"].lower()
	  state=geoIndex[geoIndex[str(geo)]["Parent"]]["Name"].lower()
      if cType=="State":
	  state=geoIndex[str(geo)]["Name"].lower()
	  city=""
      if cType=="UT":
	  state=geoIndex[str(geo)]["Name"].lower()
	  city=""
			
  if len(response.ad)>0:
    advertiserName = response.ad[0].advertiser_name[0]
    bid = int(response.ad[0].adslot[0].max_cpm_micros)
    creative=int(response.ad[0].buyer_creative_id)
    clickUrl=response.ad[0].click_through_url
    campaignId=int(response.debug_string)
    html = response.ad[0].html_snippet
    msg = url+","+str(b[0])+","+str(b[1])+","+str(country)+","+str(state)+","+str(city)+","+str(campaignId)+","+str(bid)+","+str(creative)+","+advertiserName+","+html
  else:
    msg = url+","+str(b[0])+","+str(b[1])+","+str(country)+","+str(state)+","+str(city)+",,,,,"
  print msg
  allbids=allbids+"\n"+msg

f = open('testresult.csv','w')
f.write(allbids)

conn.close()
