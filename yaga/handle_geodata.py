from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
import math
import json

bigSquareSize = 0.01
littleSquareSize = 0.005
boundaries = {"NO":{'latitude': 45.840, 'longitude': 4.680}, "SE":{'latitude': 45.730, 'longitude': 4.830}}
nbColumnLatitudeBig = (boundaries["NO"]['latitude'] - boundaries["SE"]['latitude']) / bigSquareSize #  11
nbColumnLongitudeBig = (boundaries["SE"]['longitude'] - boundaries["NO"]['longitude']) / bigSquareSize # 15
nbColumnLatitudeLittle = (boundaries["NO"]['latitude'] - boundaries["SE"]['latitude']) / littleSquareSize # 22
nbColumnLongitudeLittle = (boundaries["SE"]['longitude'] - boundaries["NO"]['longitude']) / littleSquareSize # 30

def isTimestampValid(inputStream):
	"""
	Verifies if the timestamp of the event received is within the current minute (on the server)
	@param timestamp timestamp of the event received (unix timestamp )
	@param current_timestamp current timestamp of the server (unix timestamp)
	Returns boolean
	"""
	timestamp = json.loads(inputStream)["data"]["timestamp"]
	current_timestamp = time.time()
	if not isinstance(timestamp, float) or not isinstance(current_timestamp, float):
		return False
	delta_current = abs(current_timestamp-timestamp)
	return  delta_current < 3600


def filter_region(inputStream):
	"""
	Verifies if the point given is in the region we defined
	@param point dataframe of format [{'latitude': latitude, 'longitude': longitude}]
	Return boolean
	"""
	point = json.loads(inputStream)
	point = point["data"]

	if not isinstance(point['latitude'], float or not isinstance(point['longitude'], float)):
		return False
	if point['latitude'] > boundaries["NO"]['latitude']:
		return False
	if point['latitude'] < boundaries["SE"]['latitude']:
		return False
	if point['longitude'] < boundaries["NO"]['longitude']:
		return False
	if point['longitude'] > boundaries["SE"]['longitude']:
		return False
	else:
		return True

def find_zone(point):
	"""
	Return 2 zones containing the coordinates (a little one and a big one) of a mobile phone event
	Boundaries of our map : 
	N: 45.84 degree, S: 45.73 degree, O: 4.68 degree, E: 4.83 degree, with an NS range of 0.11 degree and an EO range of 0.15 degree.
	In total we have 165 "big" squares of 0.01 degree , and  4*165=660 "little" ones of 0.005 degree 
	The first square (zone 1) in each case is at NO and the last (zone 165 for the big square 660) at SE

	@param
	Return {'bigzone': , 'littlezone': }
	"""
	data = json.loads(point)["data"]

	columnLongBigDiff = data['longitude'] - boundaries["NO"]['longitude']
	if bigSquareSize>columnLongBigDiff:
		columnLongBigDiff = 0
	columnLatBigDiff = boundaries["NO"]['latitude'] - data['latitude']
	if bigSquareSize>columnLatBigDiff:
		columnLatBigDiff = 0
	zoneBig  = columnLatBigDiff/bigSquareSize * nbColumnLongitudeBig + columnLongBigDiff/bigSquareSize

	columnLongLittleDiff = data['longitude'] - boundaries["NO"]['longitude']
	if littleSquareSize>columnLongLittleDiff:
		columnLongLittleDiff = 0
	columnLatLittleDiff = boundaries["NO"]['latitude'] - data['latitude']
	if littleSquareSize>columnLatLittleDiff:
		columnLatLittleDiff = 0
	zoneLittle  = columnLatLittleDiff/bigSquareSize * nbColumnLongitudeLittle + columnLongLittleDiff/littleSquareSize

	return json.dumps({"zone": math.floor(zoneLittle)+1, "msisdn": json.loads(point)["metadata"]["msisdn"]})
    #{'bigzone': math.floor(zoneBig) + 1,
    #return ("B"+str(math.floor(zoneBig)+1), "L"+str(math.floor(zoneLittle)+1))
# def zoneToCoord(zone, squareSize):
	
# 	latitude = zone % 11 * squareSize
# 	longitude = 
# 	return (longitude,latitude)

def streamToKeyValue(inputStream):
	"""
	Returns (# telephone, zone)
	"""
	inputStreamJSON = json.loads(inputStream)
	return (inputStreamJSON["metadata"]["msisdn"], inputStreamJSON["zones"])

def reduceByTelAndZone(element, elementNext):
	elementJSON = json.loads(element)
	elementNextJSON = json.loads(elementNext)

	if elementJSON["metadata"]["msisdn"] == elementNextJSON["metadata"]["msisdn"] and elementJSON["zones"]["littlezone"] == elementNextJSON["zones"]["littlezone"]:
		return elementNextJSON

def handlePhoneEvents():
# 	"""
# 	Feed the DictCount and DictSeen dictionnaries

# 	DictSeen
# 	<timestamp, <#tel, [idZone1, idZone2]>> 
# 	DictCount
# 	<timestamp, <idZone, count>>
# 	"""

	# Here we receive the data from kafka
	# In the Kafka parameters, you must specify either metadata.broker.list or bootstrap.servers. By default, it will start consuming from the latest offset of each Kafka partition. 
	# If you set configuration auto.offset.reset in Kafka parameters to smallest, then it will start consuming from the smallest offset.

	directKafkaStream = KafkaUtils.createDirectStream(ssc, ['geodata-6'], {"bootstrap.servers": 'localhost:9092'})  # 172.31.253.60 or localhost
	# decode json data from string
	parsedStream = directKafkaStream.map(lambda (key, value): json.loads(value))\
        .filter(isTimestampValid) \
        .filter(filter_region)\
        .map(find_zone)\
        .countByValue()\
        .map(lambda (map, count): (json.loads(map)["zone"],1))\
        .reduceByKey(lambda x, y: x+ y).pprint()
        #
        # zonedPoints =
        # zoneCounts = zonedPoints.reduce(reduceByTelAndZone)
        # zonedPoints.countByWindow().print()
        # join => for each rdd , phone in same zone at same minute ? delete , if not save and send
        #regionFilteredStreamed.pprint()


sc = SparkContext("local[*]",appName = "HandleGeoData")
ssc = StreamingContext(sc, 3)

if __name__ == "__main__":

	handlePhoneEvents()
	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate
