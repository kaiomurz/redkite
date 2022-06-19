from pyspark.sql.types import StructType, StructField, StringType, IntegerType


smallGeographySchema = StructType([
    StructField("A", StringType(), nullable=False),
    StructField("K", StringType(), nullable=False),
    StructField("L", StringType(), nullable=False),
    StructField("M", IntegerType(), nullable=False)    
])