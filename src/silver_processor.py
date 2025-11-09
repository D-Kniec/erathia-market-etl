import os
import duckdb
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path

def run_silver_processing(spark: SparkSession, bronze_path: Path, silver_path: Path, db_path: Path):
    """
    Step 2: Processes data from the Bronze layer (raw Parquet) to the Silver
    layer (cleaned Parquet), then loads it into a DuckDB database.
    """
    print("\n--- STEP 2: PROCESSING BRONZE TO SILVER ---")
    
    os.makedirs(silver_path, exist_ok=True)
    os.makedirs(db_path.parent, exist_ok=True)
    
    tables = [
        "dim_customer", "dim_product", "dim_town", "dim_faction", 
        "dim_product_category", "dim_date", "fact_sales"
    ]
    
    print(f"├── Loading Bronze data from {bronze_path}")

    dfs = {}
    for table in tables:
        path = str(bronze_path / f"{table}.parquet")
        if table == "fact_sales":
            path = str(bronze_path / "fact_sales") 
            
        if not os.path.exists(path):
            print(f"│   └── WARNING: Could not find table {table} at {path}. Skipping.")
            continue
        
        dfs[table] = spark.read.parquet(path)
        
    print("├── Cleaning and transforming data with PySpark...")

    if "fact_sales" in dfs:
        df_sales = dfs["fact_sales"]
        df_sales_clean = df_sales.na.drop(
            subset=["trade_key", "customer_key", "product_key", "date_key"]
        ).filter(F.col("quantity") != 0)
        dfs["fact_sales"] = df_sales_clean

    if "dim_customer" in dfs:
        df_customer = dfs["dim_customer"]
        df_customer_clean = df_customer.dropDuplicates(["customer_key"]).withColumn(
            "is_hero",
            F.when(F.col("customer_segment") == "VIP", True).otherwise(False)
        )
        dfs["dim_customer"] = df_customer_clean

    key_mapping = {
        "dim_product": "product_key",
        "dim_town": "town_key",
        "dim_faction": "faction_key",
        "dim_product_category": "category_key",
        "dim_date": "date_key"
    }
    
    for table_name, key in key_mapping.items():
        if table_name in dfs:
            print(f"│   ├── Deduplicating {table_name} on {key}")
            dfs[table_name] = dfs[table_name].dropDuplicates([key])

    print(f"├── Writing cleaned Parquet files to {silver_path}...")
    
    for table_name, df in dfs.items():
        output_path = str(silver_path / f"{table_name}.parquet")
        if table_name == "fact_sales":
             df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        else:
            df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    print(f"├── Ingesting Silver Parquet files into DuckDB at {db_path}...")
    
    con = duckdb.connect(database=str(db_path), read_only=False)
    
    for table_name in dfs.keys():
        parquet_path = str(silver_path / f"{table_name}.parquet")
        print(f"│   ├── Loading {table_name}...")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=1)
        """)

    print("├── Verifying data in DuckDB...")
    try:
        total_sales = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        print(f"│   └── Successfully loaded {total_sales:,.0f} transactions into DuckDB.")
    except duckdb.Error as e:
        print(f"│   └── Verification failed. Error: {e}")
        
    con.close()
    
    print("\n--- STEP 2: SILVER LAYER PROCESSING COMPLETE ---")