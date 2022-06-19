from gc import collect
import unittest
from pyspark.sql import SparkSession
# from pandas.util.testing import assert_frame_equal
from schemas.very_large_dataframe_schema import veryLargeDataframeSchema
from schemas.vld_test_schema import testVeryLargeDataframeSchema
from processing_functions.very_large_dataframe_processing \
    import processVeryLargeDataframe


class TestVLDProcessing(unittest.TestCase):
    def test_processVeryLargeDataframe(self):

        spark = SparkSession.builder.getOrCreate()

        very_large_dataframe = spark \
        .read \
        .format('csv') \
        .schema(veryLargeDataframeSchema) \
        .option("mode", "DROPMALFORMED") \
        .load('data/VeryLargeDataSet.csv', header=True)

        very_large_dataframe = processVeryLargeDataframe(very_large_dataframe)

        very_large_dataframe.show()

        test_dataframe = spark \
        .read \
        .format('csv') \
        .schema(testVeryLargeDataframeSchema) \
        .load('tests/test.csv', header=True)

        test_dataframe.show()

        # assert_frame_equal(very_large_dataframe.toPandas(),test_dataframe.toPandas())
        self.assertEqual(very_large_dataframe.collect(), test_dataframe.collect())



if __name__ == "__main__":
    unittest.main()