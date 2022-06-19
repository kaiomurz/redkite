from pyspark.sql.types import StructType, StructField, StringType, DecimalType

veryLargeDataframeSchema = StructType([
    StructField("A", StringType(), nullable=False),
    StructField("B", StringType(), nullable=False),
    StructField("C", DecimalType(precision=5, scale=2), nullable=False),
    StructField("D", DecimalType(precision=5, scale=2), nullable=False),
    StructField("E", DecimalType(precision=5, scale=2), nullable=False),
    StructField("F", DecimalType(precision=5, scale=2), nullable=True),
    StructField("G", DecimalType(precision=5, scale=2), nullable=True),
    StructField("H", DecimalType(precision=5, scale=2), nullable=True),
    StructField("I", DecimalType(precision=5, scale=2), nullable=True),
    StructField("J", DecimalType(precision=5, scale=2), nullable=True)
])

