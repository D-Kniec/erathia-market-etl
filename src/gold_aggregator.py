import duckdb
from pathlib import Path

def run_gold_aggregation(db_path: Path):
    """
    Step 3: Connects to the Silver DuckDB database, runs complex analytical
    queries, and creates aggregated Gold Layer tables (data marts).
    
    This version rounds all financials and creates clean column names
    for business-level analysis.
    """
    print("\n--- STEP 3: AGGREGATING GOLD LAYER ---")
    
    db_file = str(db_path)
    print(f"├── Connecting to Silver database: {db_file}")
    
    try:
        con = duckdb.connect(database=db_file, read_only=False)
    except duckdb.Error as e:
        print(f"│   └── ERROR: Could not connect to DuckDB. Have you run Step 2?")
        print(f"│       {e}")
        return

    # --- Query 1: Faction Economy Data Mart ---
    print(f"├── 1. Aggregating: dm_faction_economy")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dm_faction_economy AS
            WITH faction_sales AS (
                SELECT
                    f.faction_name,
                    s.transaction_type,
                    s.gold_total
                FROM fact_sales AS s
                JOIN dim_customer AS c ON s.customer_key = c.customer_key
                JOIN dim_faction AS f ON c.faction_key = f.faction_key
            )
            SELECT
                faction_name AS "Faction Name",
                ROUND(SUM(CASE WHEN transaction_type = 'BUY' THEN gold_total ELSE 0 END), 2) AS "Total Gold Spent",
                ROUND(SUM(CASE WHEN transaction_type = 'SELL' THEN gold_total ELSE 0 END), 2) AS "Total Gold Earned",
                ROUND((SUM(CASE WHEN transaction_type = 'SELL' THEN gold_total ELSE 0 END) - 
                       SUM(CASE WHEN transaction_type = 'BUY' THEN gold_total ELSE 0 END)), 2) AS "Net Profit",
                COUNT(*) AS "Total Transactions"
            FROM faction_sales
            GROUP BY faction_name
            ORDER BY "Net Profit" DESC;
        """)
        print(f"│   └── SUCCESS: Created table dm_faction_economy.")
    except duckdb.Error as e:
        print(f"│   └── ERROR creating dm_faction_economy: {e}")

    # --- Query 2: Monthly Resource Price History ---
    print(f"├── 2. Aggregating: dm_resource_price_history")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dm_resource_price_history AS
            SELECT
                d.year AS "Year",
                d.month AS "Month",
                p.product_name AS "Resource Name",
                ROUND(AVG(s.gold_per_unit), 2) AS "Average Price",
                ROUND(SUM(s.quantity), 2) AS "Total Quantity Traded"
            FROM fact_sales AS s
            JOIN dim_product AS p ON s.product_key = p.product_key
            JOIN dim_product_category AS pc ON p.category_key = pc.category_key
            JOIN dim_date AS d ON s.date_key = d.date_key
            WHERE
                pc.category_name = 'Resources'
                AND s.transaction_type = 'BUY'
            GROUP BY
                d.year, d.month, p.product_name
            ORDER BY
                "Year", "Month", "Resource Name";
        """)
        print(f"│   └── SUCCESS: Created table dm_resource_price_history.")
    except duckdb.Error as e:
        print(f"│   └── ERROR creating dm_resource_price_history: {e}")

    # --- Query 3: Top VIP Customer Value ---
    print(f"├── 3. Aggregating: dm_top_vip_customers")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dm_top_vip_customers AS
            SELECT
                c.customer_name AS "Customer Name",
                f.faction_name AS "Faction",
                ROUND(SUM(CASE WHEN s.transaction_type = 'BUY' THEN s.gold_total ELSE 0 END), 2) AS "Total Spent",
                ROUND(SUM(CASE WHEN s.transaction_type = 'SELL' THEN s.gold_total ELSE 0 END), 2) AS "Total Earned",
                COUNT(s.trade_key) AS "Total Transactions"
            FROM fact_sales AS s
            JOIN dim_customer AS c ON s.customer_key = c.customer_key
            JOIN dim_faction AS f ON c.faction_key = f.faction_key
            WHERE
                c.customer_segment = 'VIP'
            GROUP BY
                c.customer_name, f.faction_name
            ORDER BY
                "Total Spent" DESC
            LIMIT 100;
        """)
        print(f"│   └── SUCCESS: Created table dm_top_vip_customers.")
    except duckdb.Error as e:
        print(f"│   └── ERROR creating dm_top_vip_customers: {e}")
    # --- Query 4: Artifact Sales Summary ---
    print(f"├── 4. Aggregating: dm_artifact_sales_summary")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dm_artifact_sales_summary AS
            SELECT
                p.product_name AS "Artifact Name",
                pc.tier_level AS "Tier",
                COUNT(s.trade_key) AS "Total Sold",
                ROUND(SUM(s.gold_total), 2) AS "Total Gold Value"
            FROM fact_sales AS s
            JOIN dim_product AS p ON s.product_key = p.product_key
            JOIN dim_product_category AS pc ON p.category_key = pc.category_key
            WHERE
                pc.category_name = 'Artifacts'
                AND s.transaction_type = 'BUY'
            GROUP BY
                p.product_name, pc.tier_level
            ORDER BY
                "Total Sold" ASC, "Total Gold Value" DESC;
        """)
        print(f"│   └── SUCCESS: Created table dm_artifact_sales_summary.")
    except duckdb.Error as e:
        print(f"│   └── ERROR creating dm_artifact_sales_summary: {e}")

    con.close()
    print(f"└── Closed connection to {db_path}.")
    print("\n--- STEP 3: GOLD LAYER AGGREGATION COMPLETE ---")
    con.close()
    print(f"└── Closed connection to {db_path}.")
    print("\n--- STEP 3: GOLD LAYER AGGREGATION COMPLETE ---")