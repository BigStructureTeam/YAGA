import unittest, os
import yaga.handle_geodata
import time
import json


class HandlingGeoDataTestCase(unittest.TestCase):


	#def setUp(self):
		

	#def tearDown(self):
		

	# # Testing of isTimestampValid
	# def test_isTimestampValid_badInputFormat(self):
	# 	self.assertFalse(yaga.handle_geodata.isTimestampValid("otis"))


	# If the event occured at the current minute, we consider it
	def test_isTimestampValid_goodInput_same_minute(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()}})
		self.assertTrue(yaga.handle_geodata.isTimestampValid(timestamp))
		
	# If the event occured one minute ago, we don't consider it
	def test_isTimestampValid_goodInput_minute_before(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()-60}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
		


	# If the event occured at the current minute, one hour ago, we don't consider it
	def test_isTimestampValid_goodInput_minute_hour_ago(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()-3600}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
		

	# If the event occured at the current minute, a day ago, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_day_ago(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()-3600*24}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
				

	# If the event occured at the current minute, a month ago or more, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_month_ago(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()-3600*24*31}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
		
		

	# If the event occured at the current minute, a year ago, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_year_ago(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()-3600*24*365}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
				


	# If the event occured a minute ahead, we don't consider it
	def test_isTimestampValid_goodInput_minute_after(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()+60}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
		


	# If the event occured at the current minute, a hour ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_hour_ahead(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()+3600}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))


	# If the event occured at the current minute, a day ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_day_ahead(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()+3600*24}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))


	# If the event occured at the current minute, a month ahead or more, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_month_ahead(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()+3600*24*31}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
		


	# If the event occured at the current minute, a year ahead, we don't consider it
	def test_isTimestampValid_goodInput_same_minute_year_ahead(self):
		timestamp = json.dumps({'data': { "timestamp" : time.time()+3600*24*365}})
		self.assertFalse(yaga.handle_geodata.isTimestampValid(timestamp))
	
	# point located in zone 1 in both cases
	def test_zoneFinder_zone1(self):
		point = {'data': {'latitude': 45.836, 'longitude': 4.681}}
		point = json.dumps(point)

		self.assertEqual(yaga.handle_geodata.zoneFinder(point)["zones"] ,{'bigzone': 1, 'littlezone': 1})


	# point located in zone 1 in the big and zone 2 in the little
	def test_zoneFinder_zone1_zone2(self):
		point = {'data': {'latitude': 45.836, 'longitude': 4.686}}
		point = json.dumps(point)
		self.assertEqual(yaga.handle_geodata.zoneFinder(point)["zones"] ,{'bigzone': 1, 'littlezone': 2})

	# point located at the north ouest of the NO limit
	def test_regionFilter_out_no(self):
		point = {'data': {'latitude': 45.825, 'longitude': 4.675}}
		point = json.dumps(point)

		self.assertFalse(yaga.handle_geodata.regionFilter(point))

	# point located at the north east of the NE limit
	def test_regionFilter_out_ne(self):
		point = {'data': {'latitude': 45.825, 'longitude': 4.835}}
		point = json.dumps(point)

		self.assertFalse(yaga.handle_geodata.regionFilter(point))

	# point located at the south ouest of the SO limit
	def test_regionFilter_out_so(self):
		point = {'data': {'latitude': 45.841, 'longitude': 4.686}}
		point = json.dumps(point)

		self.assertFalse(yaga.handle_geodata.regionFilter(point))

	# point located at the south ouest of the SE limit
	def test_regionFilter_out_se(self):
		point = {'data': {'latitude': 45.725, 'longitude': 4.835}}
		point = json.dumps(point)

		self.assertFalse(yaga.handle_geodata.regionFilter(point))


if __name__=='__main__':
	unittest.main()