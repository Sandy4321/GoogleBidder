import realtime_bidding_proto_pb2
import httplib, urllib

url = "http://zyffg.com/map.php?id=33&jjhd=4456"
geo_criteria_id=1007751
width = 120
height = 600

request = realtime_bidding_proto_pb2.BidRequest()
request.id="1112"
request.is_ping = 0
request.google_user_id="aditya123"
request.geo_criteria_id=geo_criteria_id
request.url=url
ads = request.adslot.add()
ads.id=33
ads.width.append(width)
ads.height.append(height)

print request
requestString = request.SerializeToString()

conn = httplib.HTTPConnection("bid-hk.impulse01.com")
conn.request("POST", "", requestString)
response = conn.getresponse()
print "Status = "+str(response.status)
print "Reason = "+response.reason

data = response.read()
print "Data"+data

conn.close()
