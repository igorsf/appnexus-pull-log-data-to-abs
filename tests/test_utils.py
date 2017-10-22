__author__ = 'fridman'

import unittest

from datetime import datetime, timedelta

from common import utils

class TestUtils(unittest.TestCase):

    def setUp(self):
        None

    def test_extract_filename(self):
        path = 'pj95up2/redf5/date=2017-10-04/hour=21/impressions_f8a99254d8e9cec42a0a64adda6d0a2d.log.gz'
        filename = 'impressions_f8a99254d8e9cec42a0a64adda6d0a2d.log.gz'
        s = utils.extract_filename(path)
        self.assertEqual(s, filename)

    def test_extract_log_type(self):
        path = 'pj95up2/redf5/date=2017-10-04/hour=21/impressions_f8a99254d8e9cec42a0a64adda6d0a2d.log.gz'
        self.assertEquals('impressions', utils.extract_log_type(path))
        path = 'pj95up2/redf5aggregated/date=2017-10-21/hour=21/2017-10-2121conversions0.gz'
        self.assertEquals('conversions', utils.extract_log_type(path))

    def test_latest_date(self):
        end_date = datetime.today()
        start_date = end_date - timedelta(days=1)
        dates = utils.date_range_hourly(start_date)
        self.assertEquals(end_date, utils.latest_date(dates))

if __name__ == '__main__':
    unittest.main()
