# Databricks notebook source
MIN_SUM_THRESHOLD = 10,000,000

def process():
    # Create spark session
    spark = SparkSession.builder.getOrCreate()

    # very_large_dataframe
    # 250 GB of CSV from client which must have only 10 columns [A, B, C, D, E, F, G, H, I, J]
    # [A, B] contains string data
    # [C, D, E, F, G, H, I, J] contains decimals with precision 5, scale 2 (i.e. 125.75)
    # [A, B, C, D, E] should not be null
    # [F, G, H, I, J] should may be null

    very_large_dataset_location = 'abfss://lake@helloworld.dfs.core.windows.net/Landed/VeryLargeDataSet.csv'
    very_large_dataframe = spark.read.csv(very_large_dataset_location, header=True, sep=',') 

    # TODO
    # Column F: Need to apply conversion factor of 2.5 i.e. Value 2, conversion factor 2.5 = 5

    # Remove duplicates in column [A]
    # 50% of records in column [A] could potentially be duplicates
    very_large_dataframe = very_large_dataframe.dropDuplicates(['A'])
    
    total_sum_of_column_F = very_large_dataframe.agg(sum('F')).collect()[0][0]
    avg_of_column_F = very_large_dataframe.agg(avg('F')).collect()[0][0]

    # Ensure sum of column F is => MIN_SUM_THRESHOLD
    if total_sum_of_column_F < MIN_SUM_THRESHOLD:
        raise Exception('total_sum_of_column_F: ' + total_sum_of_column_F + ' is below threshold: ' + MIN_SUM_THRESHOLD)

    # small_geography_dimension_dataframe
    # 25 MB of parquet, 4 columns [A, K, L, M]
    # Columns [A, K, L] contain only string data
    # Column [M] is an integer
    # Columns [A, K, L, M] contain all non nullable data. Assume this is the case
    small_geography_dimension_dataset = 'abfss://lake@helloworld.dfs.core.windows.net/Gold/small_geography_dimension_dataset.parquet'
    small_geography_dimension_dataframe = spark.read.parquet(small_geography_dimension_dataset)

    # Join very_large_dataframe to small_geography_dimension_dataframe on column [A]
    # Include only column [M] from small_geography_dimension_dataframe on new very_large_dataframe
    # No data (row count) loss should occur from very_large_dataframe

    very_large_dataframe = very_large_dataframe.join(small_geography_dimension_dataframe,
                                                     (very_large_dataframe.A == small_geography_dimension_dataframe.A))

    # small_product_dimension_dataframe
    # 50 MB of parquet, 4 columns [B, N, O, P]
    # Columns [B, N] contain only string data
    # Columns [O, P] contain only integers
    # Columns [B, N, O, P] contain all non nullable data. Assume this is the case
    small_product_dimension_dataset = 'abfss://lake@helloworld.dfs.core.windows.net/Gold/small_product_dimension_dataset.parquet'
    small_product_dimension_dataframe = spark.read.parquet(small_product_dimension_dataset)

    # TODO
    # Join very_large_dataframe to small_product_dimension_dataframe on column [B]
    # Only join records to small_product_dimension_dataframe where O is greater then 10
    # Keep only Column [P]

    # Write very_large_dataframe to next stage.
    # Columns selected should be: [C, D, E, F, G, H, I, J, M, P]

    (very_large_dataframe.coalesce(1)
     .write
     .mode('overwrite')
     .format('com.databricks.spark.csv')
     .option('header', 'true')
     .option('sep', '\t').csv('./location_new'))

process()
