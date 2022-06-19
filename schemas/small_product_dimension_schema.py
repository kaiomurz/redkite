from pyspark.sql.types import StructType, StructField, StringType, IntegerType

smallProductSchema = StructType([
    StructField("B", StringType(), nullable=False),
    StructField("N", StringType(), nullable=False),
    StructField("O", IntegerType(), nullable=False),
    StructField("P", IntegerType(), nullable=False)    
])