import realtime_bidding_proto_pb2
import httplib, urllib

url = ["http://zyffg.com/map.php?id=33&jjhd=4456","","","","","","","","",""]
geo_criteria_id=[1007751,1007768,1007785,1007792,1007795,1007785]
size=[[120,600],[300,250],[160,600],[728,90]]

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

for i in range(1000):
  conn = httplib.HTTPConnection("bid-hk.impulse01.com")
  conn.request("POST", "", requestString)

conn.close()
