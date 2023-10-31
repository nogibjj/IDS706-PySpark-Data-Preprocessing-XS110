"""
Test goes here

"""
from script import descript_stat
import pandas as pd
import unittest

GY_data = pd.read_csv('Global YouTube Statistics.csv', encoding="ISO-8859-1")



#Top creators' subscriber counts, video views, upload frequency

df = GY_data[['subscribers','video views','uploads']]

class TestDescriptiveStatistics(unittest.TestCase):
    def test_function_runs_without_errors(self):
        try:
            # Call your function here
            # Example: descript_stat(your_input_data)
            descript_stat(df)
        except Exception as e:
            self.fail(f"Function raised an exception: {e}")

if __name__ == '__main__':
    unittest.main()