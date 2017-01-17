from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as psf
from pyspark.sql import SparkSession
import math
from pyspark.streaming.kafka import KafkaUtils
import json
import time
import copy
from pyspark.sql.context import SQLContext
import datetime

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName = "HandleGeoData")
#sparksession = SparkSession.builder.getOrCreate()
#spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
ssc = StreamingContext(sc, 60)



# Can be used in filter(func)
def isTimestampValid(inputStream):
	""" 
	Verifies if the timestamp of the event received is within the current minute (on the server) 
	@param timestamp timestamp of the event received (unix timestamp ) 
	@param current_timestamp current timestamp of the server (unix timestamp) 
	Returns boolean
	"""

	timestamp = json.loads(inputStream)
	timestamp = timestamp["data"]["timestamp"]

	current_timestamp = time.time()

	if (not isinstance(timestamp, float) or not isinstance(current_timestamp, float)):
		return False


	delta_current = datetime.timedelta(abs(current_timestamp-timestamp))

	return delta_current.days == 0 and delta_current.seconds < 60


# first element is NO, second is SE
boundaries = [{'latitude': 45.840, 'longitude': 4.680}, {'latitude': 45.730, 'longitude': 4.830}]
bigSquareSize = 0.01
littleSquareSize = 0.005

def regionFilterJSON(point):

	return not regionFilter(point)


# Can be used in filter(func)
def regionFilter(inputStream):
	"""
	Verifies if the point given is in the region we defined
	@param point dataframe of format [{'latitude': latitude, 'longitude': longitude}]
	Return boolean
	"""
	point = json.loads(inputStream)
	point = point["data"]

	if (not isinstance(point['latitude'], float) or not isinstance(point['longitude'], float)):
		return False
	return point['latitude'] <= boundaries[0]['latitude'] and point['latitude'] >= boundaries[1]['latitude'] and point['longitude'] >= boundaries[0]['longitude'] and point['longitude'] <= boundaries[1]['longitude']

# Can be used in map(func)
def zoneFinderJSON(point):
	point = json.loads(point)
	point = point["data"]
	return zoneFinder(point) 

def zoneFinder(point):
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

	nbColumnLatitudeBig = (boundaries[0]['latitude'] - boundaries[1]['latitude']) / bigSquareSize #  11
	nbColumnLongitudeBig = (boundaries[1]['longitude'] - boundaries[0]['longitude']) / bigSquareSize # 15


	nbColumnLatitudeLittle = (boundaries[0]['latitude'] - boundaries[1]['latitude']) / littleSquareSize # 22
	nbColumnLongitudeLittle = (boundaries[1]['longitude'] - boundaries[0]['longitude']) / littleSquareSize # 30
	

	columnLongBigDiff = abs(data['longitude'] - boundaries[0]['longitude'] ) 
	if bigSquareSize>columnLongBigDiff:
		columnLongBigDiff = 0
	columnLatBigDiff = abs(boundaries[0]['latitude'] - data['latitude']) 
	if bigSquareSize>columnLatBigDiff:
		columnLatBigDiff = 0
	zoneBig  = columnLatBigDiff/bigSquareSize * nbColumnLongitudeBig + columnLongBigDiff/bigSquareSize



	columnLongLittleDiff = abs(data['longitude'] - boundaries[0]['longitude'] ) 
	if littleSquareSize>columnLongLittleDiff:
		columnLongLittleDiff = 0
	columnLatLittleDiff = abs(boundaries[0]['latitude'] - data['latitude']) 
	if littleSquareSize>columnLatLittleDiff:
		columnLatLittleDiff = 0
	zoneLittle  = columnLatLittleDiff/bigSquareSize * nbColumnLongitudeLittle + columnLongLittleDiff/littleSquareSize

	data["zones"] = {'bigzone': math.floor(zoneBig)+1, 'littlezone': math.floor(zoneLittle)+1}

	#return ("B"+str(math.floor(zoneBig)+1), "L"+str(math.floor(zoneLittle)+1))
	print data
	return data

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



def isTimestampValidJSON(inputStream):

	return not isTimestampValid(inputStream)
	# return False

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
	parsedStream = directKafkaStream.map(lambda (key, value): json.loads(value)) # after that operation it's a regular Dstream
	timeFilteredStream = parsedStream.filter(isTimestampValidJSON) 
	regionFilteredStreamed = timeFilteredStream.filter(regionFilterJSON)
	# zonedPoints = regionFilteredStreamed.map(zoneFinder)
	# zoneCounts = zonedPoints.reduce(reduceByTelAndZone)


	# zonedPoints.countByWindow().print()

	# join => for each rdd , phone in same zone at same minute ? delete , if not save and send 
	regionFilteredStreamed.pprint()

if __name__ == "__main__":

	handlePhoneEvents()
	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate
