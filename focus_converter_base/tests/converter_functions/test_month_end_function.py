from unittest import TestCase


class TestMonthStartFunction(TestCase):
    """
    Test the month_start function
    """

    def test_month_start(self):
        """
        Test the month_start function
        :return:
        """
        from focus_converter_base.focus_converter.conversion_functions import (
            month_start,
        )
        from datetime import datetime

        # test with a datetime object
        dt = datetime(2020, 1, 1, 0, 0, 0)
        dt_start = month_start(dt)
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string
        dt = "2020-01-01 00:00:00"
        dt_start = month_start(dt)
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "2020-01-01"
        dt_start = month_start(dt, format="%Y-%m-%d")
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "01/01/2020"
        dt_start = month_start(dt, format="%m/%d/%Y")
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "01/01/2020"
        dt_start = month_start(dt, format="%d/%m/%Y")
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "01/01/2020"
        dt_start = month_start(dt, format="%d/%m/%Y")
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "01/01/2020"
        dt_start = month_start(dt, format="%d/%m/%Y")
        self.assertEqual(dt_start, datetime(2020, 1, 1, 0, 0, 0))

        # test with a string and format
        dt = "01/01/2020"
