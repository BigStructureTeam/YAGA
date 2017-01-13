from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as psf
from pyspark.sql import SparkSession
import math
from pyspark.streaming.kafka import KafkaUtils
import json
import time
import copy

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName = "HandleGeoData")
sparksession = SparkSession.builder.getOrCreate()
#spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
ssc = StreamingContext(sc, 1)



# Can be used in filter(func)
def isTimestampValid(timestamp):
	""" 
	Verifies if the timestamp of the event received is within the current minute (on the server) 
	@param timestamp timestamp of the event received (unix timestamp ) 
	@param current_timestamp current timestamp of the server (unix timestamp) 
	Returns boolean
	"""
	current_timestamp = time.time()
	timestamp = json.loads(timestamp)

	print(timestamp)

	if (not isinstance(timestamp, float) or not isinstance(current_timestamp, float)):
		return False
	current_time  = sparksession.createDataFrame([(current_timestamp,)], ['timestamp'])
	current_time = current_time.withColumn('timestamp',current_time.timestamp.cast("timestamp"))
	current_minute = current_time.select(psf.minute('timestamp').alias('minute')).collect()
	current_hour = current_time.select(psf.hour('timestamp').alias('hour')).collect()
	current_day = current_time.select(psf.dayofmonth('timestamp').alias('day')).collect()
	current_month = current_time.select(psf.month('timestamp').alias('month')).collect()
	current_year = current_time.select(psf.year('timestamp').alias('year')).collect()

	timestamp_copy = copy.deepcopy(timestamp)
	event_time = sparksession.createDataFrame([(timestamp_copy,)], ['timestamp']) # problem here
	event_time = event_time.withColumn('timestamp', event_time.timestamp.cast("timestamp"))
	event_minute = event_time.select(psf.minute('timestamp').alias('minute')).collect()
	event_hour = event_time.select(psf.hour('timestamp').alias('hour')).collect()
	event_day = event_time.select(psf.dayofmonth('timestamp').alias('day')).collect()
	event_month = event_time.select(psf.month('timestamp').alias('month')).collect()
	event_year = event_time.select(psf.year('timestamp').alias('year')).collect()

	# return False
	return current_minute ==  event_minute and current_hour == event_hour and current_day == event_day  and current_month == event_month and current_year == event_year


# first element is NO, second is SE
boundaries = [{'latitude': 45.840, 'longitude': 4.680}, {'latitude': 45.730, 'longitude': 4.830}]
#boundariesdf = sparksession.createDataFrame([(boundaries,)], ['latitude','longitude'])
bigSquareSize = 0.01
littleSquareSize = 0.005

# Can be used in filter(func)
def regionFilter(point):
	"""
	Verifies if the point given is in the region we defined
	@param point dataframe of format [{'latitude': latitude, 'longitude': longitude}]
	Return boolean
	"""
	if (not isinstance(point['latitude'], float) or not isinstance(point['longitude'], float)):
		return False
	return point['latitude'] <= boundariesdf[0]['latitude'] and point['latitude'] >= boundariesdf[1]['latitude'] and point['longitude'] >= boundariesdf[0]['longitude'] and point['longitude'] <= boundariesdf[1]['longitude']

# Can be used in map(func)
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
	point = json.loads(point)
	point = point["data"]

	nbColumnLatitudeBig = (boundaries[0]['latitude'] - boundaries[1]['latitude']) / bigSquareSize #  11
	nbColumnLongitudeBig = (boundaries[1]['longitude'] - boundaries[0]['longitude']) / bigSquareSize # 15


	nbColumnLatitudeLittle = (boundaries[0]['latitude'] - boundaries[1]['latitude']) / littleSquareSize # 22
	nbColumnLongitudeLittle = (boundaries[1]['longitude'] - boundaries[0]['longitude']) / littleSquareSize # 30
	

	columnLongBigDiff = abs(point['longitude'] - boundaries[0]['longitude'] ) 
	if bigSquareSize>columnLongBigDiff:
		columnLongBigDiff = 0
	columnLatBigDiff = abs(boundaries[0]['latitude'] - point['latitude']) 
	if bigSquareSize>columnLatBigDiff:
		columnLatBigDiff = 0
	zoneBig  = columnLatBigDiff/bigSquareSize * nbColumnLongitudeBig + columnLongBigDiff/bigSquareSize



	columnLongLittleDiff = abs(point['longitude'] - boundaries[0]['longitude'] ) 
	if littleSquareSize>columnLongLittleDiff:
		columnLongLittleDiff = 0
	columnLatLittleDiff = abs(boundaries[0]['latitude'] - point['latitude']) 
	if littleSquareSize>columnLatLittleDiff:
		columnLatLittleDiff = 0
	zoneLittle  = columnLatLittleDiff/bigSquareSize * nbColumnLongitudeLittle + columnLongLittleDiff/littleSquareSize


	return {'bigzone': math.floor(zoneBig)+1, 'littlezone': math.floor(zoneLittle)+1}

def isTimestampValidJSON(a):
	a = json.loads(a)
	print(a)
	b = a["data"]["timestamp"]


	return isTimestampValid(b)

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
	directKafkaStream = KafkaUtils.createDirectStream(ssc, ['geodata'], {"bootstrap.servers": 'localhost:9092'}) 
	# decode json data from string
	parsedStream = directKafkaStream.map(lambda (key, value): json.loads(value))
	parsedStream.filter(lambda json: isTimestampValidJSON(json)) # we don't know what to do here => if we don't assign parsedStream,
	# the filter is not executed. If we do it, se get  PicklingError: Could not serialize object: Py4JError: Method __getnewargs__([]) does not exist 
	
	#parsedStream.filter(lambda json: regionFilter(json["data"]))
	#parsedStream.pprint()
	zonedPoints = parsedStream.map(lambda json: zoneFinder(copy.deepcopy(json)))
	zonedPoints.pprint()


if __name__ == "__main__":

	handlePhoneEvents()
	ssc.start()             # Start the computation
	ssc.awaitTermination()  # Wait for the computation to terminate
