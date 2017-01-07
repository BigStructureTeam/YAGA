from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pyspark.sql

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "HandleGeoData")
ssc = StreamingContext(sc, 1)



def isTimestampValid(timestamp):
	"""
	Verifies if the timestamp of the event received is within the current minute (on the server)

	Return boolean
	"""
	df = pyspark.sql.functions.from_unixtime(timestamp, format='yyyy-MM-dd HH:mm:ss')
	receivedFrame = spark.createDataFrame([df], ['a'])
	currentFrame = pyspark.sql.functions.current_date()

	

def zoneFilter(lat, long):
	"""
	Return 2 zones containing the coordinates (a little one and a big one) of a mobile phone event or nothing if it's outside our general area
	"""


def handlePhoneEvent():
	"""
	Feed the DictCount and DictSeen dictionnaries

	DictSeen
	<timestamp, <#tel, [idZone1, idZone2, …]>> 

	DictCount
	<timestamp, <idZone, count>>
	"""





# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate