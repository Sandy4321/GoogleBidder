import time
import json
import sys
import timeit
from random import choice
import random


domains=['911tabs.com','2games.com','37millionminutes.com','2dplay.com','123greetings.com','9jatube.tv','aapplemint.com','abroy.com','accesshollywood.com','accuweather.com','adobetutorialz.com','adverblog.com','africam.com','agame.com','ajc.com','al.com','albinoblacksheep.com','alfy.com','all-allergies.com','allsportpass.com','americangreetings.com','americanpregnancy.org','androidcentral.com','answers.com','anyclip.com','apps.facebook.com','armorgames.com','artistdirect.com','athlonsports.com','autoblog.com','autofederation.com','autosport.com','awesomemom.tv','babble.com','babycenter.com','babymed.com','babynames.com','ballerarcade.com','baltimoresun.com','bebo.com','better.tv','betterrecipes.com','beyondthedow.com','bgames.com','bhg.com','bigdino.com','bighealthtree.com','bigsoccer.com','billboard.com','blastro.com','bleacherreport.com','blinkx.com','blip.tv','blisstree.com','blogtalkradio.com','blogtv.com','bmi.tv','bnqt.com','bodyarchitect.tv','bookrags.com','bored.com','boston.com','bostonherald.com',
'breitbart.tv','brightdeal.com','britannica.com','broadbandsports.com','broadway.tv','bubblebox.com','buddytv.com','businessinsider.com','buydesignerfashions.com','buzzfocus.com','buzzlol.com','candystand.com','cappex.com','care2.com','cartoondollemporium.com','cartown.com','celebrity-gossip.net','celebuzz.com','cfo.com','characterarcade.com','cheapstuff.com','cheezburger.com','chess.com','chicagotribune.com','chinaflix.com','chinaontv.com','christianpost.com','cinesport.com','citationmachine.net','cityspur.com','cleveland.com','clipsyndicate.com','collegehumor.com','collegeprowler.com','collegepublisher.com','come2play.com','comingsoon.net','computing.net','connectedinternet.co.uk','contactmusic.com','cookingclub.com','cookinggames.com','coolmom.com','countryhome.com','crackberry.com','crackle.com','crispygamer.com','crunchyroll.com','crushable.com','cruzetalk.com','cyclingdirt.org','dailyfreegames.com','dailymail.co.uk','dashrecipes.com','descubrearte.com','destructoid.com','detnews.com','deviantart.com',
'devidomainemagdomainsings.com','dictionary.com','digitaltrends.com']

states=['Andaman and Nicobar Islands',
'Andhra Pradesh',
'Assam',
'Bihar',
'Delhi',
'Gujarat',
'Haryana',
'Jammu and Kashmir',
'Karnataka',
'Kerala',
'Maharashtra',
'Meghalaya',
'Madhya Pradesh',
'Odisha',
'Punjab',
'Pondicherry',
'Rajasthan',
'Tamil Nadu',
'Tripura',
'Uttar Pradesh',
'West Bengal',
'Goa',
'Arunachal Pradesh',
'Chhattisgarh',
'Himachal Pradesh',
'Jharkhand',
'Manipur',
'Mizoram',
'Nagaland',
'Sikkim',
'Uttarakhand']

cities=['Hyderabad',
'Nellore',
'Vijayawada',
'Visakhapatnam',
'Warangal',
'Guwahati',
'Silchar',
'Dhanbad',
'Jamshedpur',
'Patna',
'New Delhi',
'Ahmedabad',
'Anand',
'Gandhinagar',
'Jamnagar',
'Rajkot',
'Surat',
'Vadodara',
'Faridabad',
'Gurgaon',
'Srinagar',
'Bangalore',
'Belgaum',
'Dharwad',
'Mangalore',
'Mysuru',
'Kochi',
'Kottayam',
'Kozhikode',
'Thiruvananthapuram',
'Aurangabad',
'Kolhapur',
'Mumbai',
'Nagpur',
'Nanded',
'Pune',
'Solapur',
'Shillong',
'Bhopal',
'Bilaspur',
'Gwalior',
'Indore',
'Jabalpur',
'Bhubaneswar',
'Amritsar',
'Chandigarh',
'Ludhiana',
'Sangrur',
'Puducherry',
'Jaipur',
'Kota',
'Udaipur',
'Chennai',
'Coimbatore',
'Erode',
'Madurai',
'Sivakasi',
'Thanjavur',
'Vellore',
'Allahabad',
'Dehradun',
'Ghaziabad',
'Haldwani',
'Kanpur',
'Lucknow',
'Mathura',
'Noida',
'Varanasi',
'Kolkata',
'Tezpur',
'Imphal',
'Shimla',
'Moradabad',
'Meerut',
'Sonepat',
'Karnal',
'Roorkee',
'Patiala',
'Pilani',
'Jalandhar',
'Kangra',
'Jammu',
'Jodhpur',
'Nadiad',
'Bhavnagar',
'Ajmer',
'Alwar',
'Agra',
'Aligarh',
'Jhansi',
'Sagar',
'Satna',
'Gorakhpur',
'Bareilly',
'Siliguri',
'Bokaro Steel City',
'Ranchi',
'Durgapur',
'Barddhaman',
'Cuttack',
'Rourkela',
'Sambalpur',
'Raipur',
'Bhilai',
'Eluru',
'Rajahmundry',
'Kakinada',
'Brahmapur',
'Guntur',
'Tirupati',
'Sembakkam',
'Irumbuliyur',
'Virudhunagar',
'Tirunelveli',
'Kollam',
'Mavelikkara',
'Ernakulam',
'Alappuzha',
'Kannur',
'Thrissur',
'Palakkad',
'Tiruchirappalli',
'Salem',
'Chittoor',
'Hosur',
'Anantapur',
'Kurnool',
'Bellary',
'Hubballi',
'Manipal',
'Margao',
'Panaji',
'Mormugao',
'Pimpri Chinchwad',
'Bijapur',
'Secunderabad',
'Karimnagar',
'Latur',
'Jalgaon',
'Nashik',
'Dhule',
'Vapi',
'Ambernath',
'Ulhasnagar',
'Dombivli',
'Kalyan',
'Virar',
'Vasai',
'Mira Bhayandar',
'Thane',
'Navi Mumbai']

isps=["Airtel","Vodafone","MTNL","Reliance Broadband","Tata"]

ruleSet=dict()

for i in range(100000):
  r = random.randrange(0,100)
  if r<30:
    domain = choice(domains)
  else:
    domain ="*"

  r = random.randrange(0,100)
  if r<30:
    city = choice(cities)
  else:
    city ="*"

  r = random.randrange(0,100)
  if r<30:
    state = choice(states)
  else:
    state ="*"

  r = random.randrange(0,100)
  if r<10:
    weekday = random.randrange(1,7)
  else:
    weekday ="*"

  r = random.randrange(0,100)
  if r<10:
    hour = random.randrange(0,23)
  else:
    hour ="*"
    
  r = random.randrange(0,100)
  if r<10:
    daypart = random.randrange(1,6)
  else:
    daypart ="*"
    
  r = random.randrange(0,100)
  if r<10:
    size = random.randrange(1,4)
  else:
    size ="*"
    
  r = random.randrange(0,100)
  if r<10:
    isp = choice(isps)
  else:
    isp ="*"
    
  rule =domain.lower()+"|"+city.lower()+"|"+state.lower()+"|"+str(weekday)+"|"+str(hour)+"|"+str(daypart)+"|"+str(size)+"|"+str(isp).lower()
  bid=random.randrange(1,100)
  ruleSet[rule]=bid

text_file = open("rules.json", "w")
text_file.write(json.dumps(ruleSet))
text_file.close()