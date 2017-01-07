import unittest, os
import yaga.handle_geodata
import time


class HandlingGeoDataTestCase(unittest.TestCase):


	#def setUp(self):
		

	#def tearDown(self):
		

	# Testing of isTimestampValid
	def test_isTimestampValid_badInputFormat(self):
		self.assertFalse(yaga.handle_geodata.isTimestampValid("otis","redding"))


	# If the event occured at the current minute, we consider it
	def test_isTimestampValid_goodInput_same_minute(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp,current_timestamp))
		
	# If the event occured one minute ago, we don't consider it
	def test_isTimestampValid_goodInput_minute_before(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp-60, current_timestamp))
		


	# If the event occured at the current minute, one hour ago, we don't consider it
	def test_isTimestampValid_goodInput_minute_hour_ago(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp-3600, current_timestamp))
		

	# If the event occured at the current minute, a day ago, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_day_ago(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp-3600*24, current_timestamp))
				

	# If the event occured at the current minute, a month ago or more, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_month_ago(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp-3600*24*31, current_timestamp))
		
		

	# If the event occured at the current minute, a year ago, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_year_ago(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp-3600*24*365, current_timestamp))
				


	# If the event occured a minute ahead, we don't consider it
	def test_isTimestampValid_goodInput_minute_after(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp+60, current_timestamp))
		


	# If the event occured at the current minute, a hour ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_hour_ahead(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp+3600, current_timestamp))


	# If the event occured at the current minute, a day ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_day_ahead(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp+3600*24, current_timestamp))


	# If the event occured at the current minute, a month ahead or more, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_month_ahead(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp+3600*24*31, current_timestamp))
		


	# If the event occured at the current minute, a year ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_year_ahead(self):
		current_timestamp = time.time()
		self.assertFalse(yaga.handle_geodata.isTimestampValid(current_timestamp+3600*24*365, current_timestamp))
		


if __name__=='__main__':
	unittest.main()