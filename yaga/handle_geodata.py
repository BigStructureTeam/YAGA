from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as psf
from pyspark.sql import SparkSession

# Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "HandleGeoData")
sc = SparkSession.builder.getOrCreate()
#spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
#ssc = StreamingContext(sc, 1)


def isTimestampValid(timestamp, current_timestamp):
	""" 
	Verifies if the timestamp of the event received is within the current minute (on the server) 
	@param timestamp timestamp of the event received (unix timestamp ) 
	@param current_timestamp current timestamp of the server (unix timestamp) 
	Returns boolean
	"""
	if (not isinstance(timestamp, int) or not isinstance(timestamp, int)):
		return False
	current_time  = sc.createDataFrame([(current_timestamp,)], ['timestamp'])
	current_time = current_time.withColumn('timestamp',current_time.timestamp.cast("timestamp"))
	current_minute = current_time.select(psf.minute('timestamp').alias('minute')).collect()
	current_hour = current_time.select(psf.hour('timestamp').alias('hour')).collect()
	current_month = current_time.select(psf.month('timestamp').alias('month')).collect()
	current_year = current_time.select(psf.year('timestamp').alias('year')).collect()

	event_time = sc.createDataFrame([(timestamp,)], ['timestamp'])
	event_time = event_time.withColumn('timestamp', event_time.timestamp.cast("timestamp"))
	event_minute = event_time.select(psf.minute('timestamp').alias('minute')).collect()
	event_hour = event_time.select(psf.hour('timestamp').alias('hour')).collect()
	event_month = event_time.select(psf.month('timestamp').alias('month')).collect()
	event_year = event_time.select(psf.year('timestamp').alias('year')).collect()


	return current_minute ==  event_minute and current_hour == event_hour and current_month == event_month and current_year == event_year
	

#def zoneFilter(lat, long):
	"""
	Return 2 zones containing the coordinates (a little one and a big one) of a mobile phone event or nothing if it's outside our general area
	"""


#def handlePhoneEvent():
	"""
	Feed the DictCount and DictSeen dictionnaries

	DictSeen
	<timestamp, <#tel, [idZone1, idZone2]>> 
	DictCount
	<timestamp, <idZone, count>>
	"""






# Create a DStream that will connect to hostname:port, like localhost:9999
#lines = ssc.socketTextStream("localhost", 9999)
#ssc.start()             # Start the computation
#ssc.awaitTermination()  # Wait for the computation to terminate
