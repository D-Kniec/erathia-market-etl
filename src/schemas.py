from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, LongType
)

FACT_SALES_SCHEMA = StructType([
    StructField("trade_key", LongType(), False), 
    StructField("date_key", IntegerType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("customer_key", LongType(), False),
    StructField("product_key", LongType(), False),
    StructField("town_key", LongType(), False),
    StructField("quantity", DoubleType(), True),
    StructField("gold_per_unit", DoubleType(), True),
    StructField("gold_total", DoubleType(), True),
    StructField("current_gold_balance", DoubleType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
])