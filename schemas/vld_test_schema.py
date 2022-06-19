from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DoubleType

testVeryLargeDataframeSchema = StructType([
    StructField("A", StringType(), nullable=False),
    StructField("B", StringType(), nullable=False),
    StructField("C", DecimalType(precision=5, scale=2), nullable=False),
    StructField("D", DecimalType(precision=5, scale=2), nullable=False),
    StructField("E", DecimalType(precision=5, scale=2), nullable=False),
    StructField("F", DoubleType(), nullable=True),
    StructField("G", DecimalType(precision=5, scale=2), nullable=True),
    StructField("H", DecimalType(precision=5, scale=2), nullable=True),
    StructField("I", DecimalType(precision=5, scale=2), nullable=True),
    StructField("J", DecimalType(precision=5, scale=2), nullable=True)
])


# root
#  |-- A: string (nullable = true)
#  |-- B: string (nullable = true)
#  |-- C: decimal(5,2) (nullable = true)
#  |-- D: decimal(5,2) (nullable = true)
#  |-- E: decimal(5,2) (nullable = true)
#  |-- F: double (nullable = true)
#  |-- G: decimal(5,2) (nullable = true)
#  |-- H: decimal(5,2) (nullable = true)
#  |-- I: decimal(5,2) (nullable = true)
#  |-- J: decimal(5,2) (nullable = true)