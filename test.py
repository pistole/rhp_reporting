#!/usr/bin/env python

import unittest
from rhp_reporting import *


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.warehouse = load_file('testconfig.yaml')
    def test_run_reports(self):
        with connect() as conn:
            with conn.cursor() as cursor:
                for report in self.warehouse.reports:
                    q = build_query(self.warehouse, report)
                    cursor.execute(q["query"], q["params"])


if __name__ == "__main__":
    unittest.main()
