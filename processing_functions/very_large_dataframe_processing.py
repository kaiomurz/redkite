
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
import pandas as pd


def processVeryLargeDataframe(very_large_dataframe: SparkDataFrame)\
    -> SparkDataFrame:
    """
    Process very_large_dataframe according to specifications
    """
    very_large_dataframe = very_large_dataframe\
    .withColumn("F", col("F")*2.5)\
    .dropDuplicates(['A'])

    print("Column F multiplied by 2.5. \nDuplicates in column A dropped\n")
    very_large_dataframe.printSchema()

    return very_large_dataframe