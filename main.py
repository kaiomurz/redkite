from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql import DataFrame as SparkDataFrame

from schemas.very_large_dataframe_schema import veryLargeDataframeSchema
from schemas.small_geography_dimension_schema import smallGeographySchema
from schemas.small_product_dimension_schema import smallProductSchema

from processing_functions.very_large_dataframe_processing \
    import processVeryLargeDataframe


MIN_SUM_THRESHOLD = 10_000_000

spark = SparkSession.builder.getOrCreate()
# spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

def load_dataframes() -> Tuple [SparkDataFrame,
                                SparkDataFrame,
                                SparkDataFrame]:
    """
    Loads all the dataframes needed for the task. 
    """

    very_large_dataframe = spark \
    .read \
    .format('csv') \
    .schema(veryLargeDataframeSchema) \
    .option("mode", "DROPMALFORMED") \
    .load('data/VeryLargeDataSet.csv', header=True)


    small_geography_dimension_dataframe = spark \
    .read \
    .format('parquet') \
    .load('data/small_geography_dimension_dataset.parquet')

    small_product_dimension_dataframe = spark \
    .read \
    .format('parquet') \
    .load('data/small_product_dimension_dataset.parquet')

    return very_large_dataframe,\
        small_geography_dimension_dataframe,\
        small_product_dimension_dataframe


def process(very_large_dataframe: SparkDataFrame, \
    small_geography_dimension_dataframe: SparkDataFrame, \
    small_product_dimension_dataframe: SparkDataFrame) \
    -> None:
    """
    Process the dataframes according to requirements
    """
    print("Processing dataframes\n")

    # process very_large_dataframe
    very_large_dataframe = processVeryLargeDataframe(very_large_dataframe)
    # compute and print required aggregate functions
    total_sum_of_column_F = very_large_dataframe.agg(_sum('F')).collect()[0][0]
    print(f"Column F total: {total_sum_of_column_F:.2f}")
    avg_of_column_F = very_large_dataframe.agg(avg('F')).collect()[0][0]
    print(f"Average of column F: {avg_of_column_F:.2f}\n")

    # check whether minimum threshold was achieved 
    # (uncomment to test)
    # if total_sum_of_column_F < MIN_SUM_THRESHOLD:
    #     raise Exception('total_sum_of_column_F: ' + str(total_sum_of_column_F)\
    #                       + ' is below threshold: ' + str(MIN_SUM_THRESHOLD))

    # join small_geography_dimension_dataframe to very_large_dataframe as specified
    very_large_dataframe = \
    very_large_dataframe\
    .join(small_geography_dimension_dataframe,\
        on=["A"],\
        how="left")\
    .drop("K", "L")

    print("VLD after joining geography dimension dataframe as specified")
    very_large_dataframe.show(5)

    # join small_product_dimension_dataframe to very_large_dataframe as specified
    very_large_dataframe = \
    very_large_dataframe\
    .join(small_product_dimension_dataframe, \
        on="B", \
        how="inner")\
    .where(col("O")>10)\
    .drop("N","O")

    print("VLD after joining product dimension dataframe as specified")
    very_large_dataframe.show(5)

    very_large_dataframe_pd = very_large_dataframe.toPandas()
    very_large_dataframe_pd.to_csv('data/processed_vld.csv', sep="\t", index=False)
    print("Processed VLD saved\n")



if __name__ == "__main__":

    # load dataframes
    very_large_dataframe, \
    small_geography_dimension_dataframe, \
    small_product_dimension_dataframe \
    = load_dataframes()

    # sanity check for whether dataframes have been loaded appropriately
    print("dataframes loaded\n")
    print("very_large_dataframe")
    very_large_dataframe.show(5)  
    print("small_geography_dimension_dataframe")
    small_geography_dimension_dataframe.show(5)
    print("small_product_dimension_dataframe")
    small_product_dimension_dataframe.show(5)

    # process the dataframes
    process(very_large_dataframe, \
    small_geography_dimension_dataframe, \
    small_product_dimension_dataframe)