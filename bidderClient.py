import realtime_bidding_proto_pb2
import httplib, urllib
import random

#url = ["http://zyffg.com/map.php?id=33&jjhd=4456","http://moneycontrol.com","http://yahoo.com/hell.php","http://tornadoweb.com"]
#geo_criteria_id=[1007751,1007753,1007765,1007772,1007788,1007805,1007809,1007809,9040183]

url="http://moneycontrol.com"
geo_criteria_id = 1007751
size=[[120,600],[160,600],[300,250],[728,90]]

request = realtime_bidding_proto_pb2.BidRequest()
request.id="1112"
request.is_ping = 0
request.google_user_id="aditya123"
request.geo_criteria_id=geo_criteria_id
request.url=url
ads = request.adslot.add()
ads.id=33
b=random.choice(size)
ads.width.append(b[0])
ads.height.append(b[1])

print request
requestString = request.SerializeToString()

conn = httplib.HTTPConnection("124.248.207.109:8888")
for i in range(1000):
  print "making request "+str(i)
  conn.request("POST", "/getbid", requestString)
  response = conn.getresponse()
  print response.status, response.reason
  data = response.read()
  print data

conn.close()
