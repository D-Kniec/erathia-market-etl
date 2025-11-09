from pyspark.sql import SparkSession
import sys
import os
import shutil
from pathlib import Path
import duckdb
import pandas as pd

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_rows', 100)

src_path = os.path.join(os.path.dirname(__file__), 'src')
sys.path.append(src_path)

try:
    import config 
    from bronze_generator import run_bronze_generation
    from silver_processor import run_silver_processing 
    from gold_aggregator import run_gold_aggregation
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"Please ensure all modules (bronze_generator, silver_processor, gold_aggregator) exist in {src_path}")
    sys.exit(1)

def init_spark():
    """Initializes and returns a Spark Session."""
    print("├── Initializing Spark Session...")
    return (
        SparkSession.builder
        .appName("Heroes3_ETL")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .master("local[*]") 
        .getOrCreate()
    )

def get_numeric_input(prompt, default, input_type=float):
    """Helper to get validated numeric input from user."""
    while True:
        try:
            value_str = input(f"  > {prompt} (default: {default}): ")
            if not value_str:
                return default
            return input_type(value_str)
        except ValueError:
            print(f"  Error: Please enter a valid number ({input_type.__name__}).")

def get_simulation_params():
    """Prompts user for simulation parameters."""
    print("\n├── Configure Simulation Parameters (Press Enter for defaults):")
    
    cost_of_living = get_numeric_input(
        "Cost of Living (% of income, e.g., 0.5)", 
        default=config.DEFAULT_COST_OF_LIVING, 
        input_type=float
    )
    
    wealth_tax_rate = get_numeric_input(
        "Wealth Tax (% of balance, e.g., 0.05)",
        default=config.DEFAULT_WEALTH_TAX,
        input_type=float
    )
    
    print("│\n├── VIP (Hero) Action Chances:")
    vip_buy_chance = get_numeric_input("  BUY chance (e.g., 0.5)", 0.5, float)
    vip_sell_chance = get_numeric_input("  SELL chance (e.g., 0.2)", 0.2, float)
    
    print("│\n├── Standard (Unit) Action Chances:")
    unit_buy_chance = get_numeric_input("  BUY chance (e.g., 0.2)", 0.2, float)
    unit_sell_chance = get_numeric_input("  SELL chance (e.g., 0.6)", 0.6, float)

    params = {
        'cost_of_living_rate': cost_of_living,
        'wealth_tax_rate': wealth_tax_rate,
        'vip_buy_chance': vip_buy_chance,
        'vip_sell_chance': vip_sell_chance,
        'unit_buy_chance': unit_buy_chance,
        'unit_sell_chance': unit_sell_chance,
    }
    
    if (vip_buy_chance + vip_sell_chance) > 1.0 or (unit_buy_chance + unit_sell_chance) > 1.0:
        print("\n  ERROR: Sum of BUY and SELL chances cannot exceed 1.0. Using defaults.")
        return {
            'cost_of_living_rate': config.DEFAULT_COST_OF_LIVING,
            'wealth_tax_rate': config.DEFAULT_WEALTH_TAX,
            'vip_buy_chance': 0.5, 'vip_sell_chance': 0.2,
            'unit_buy_chance': 0.2, 'unit_sell_chance': 0.6,
        }
        
    print("└── Simulation parameters set.")
    return params

def get_week_count():
    """Prompts user for number of weeks to simulate."""
    default_weeks = 104 
    
    while True:
        try:
            prompt = f"  > How many weeks to simulate? (default: {default_weeks}): "
            count_str = input(prompt)
            
            if not count_str:
                weeks = default_weeks
            else:
                weeks = int(count_str)
            
            if weeks <= 0:
                print("  Error: Number of weeks must be positive.")
            else:
                avg_agents = 300 
                shopping_agents = avg_agents * 0.5 
                transactions = shopping_agents * 0.7 
                estimated_rows = int(weeks * transactions)
                
                print(f"  OK. Simulating {weeks} weeks.")
                print(f"  Estimated transactions: ~{estimated_rows:,.0f} rows.")
                return weeks
        except ValueError:
            print("  Error: Please enter a valid integer.")

def handle_generate_bronze():
    """Handler for Step 1: Generate Bronze Layer."""
    print(f"\nConfiguring Step 1 (Bronze)...")
    
    sim_weeks = get_week_count()
    sim_params = get_simulation_params()
    
    counts = {
        'weeks': sim_weeks,
        'params': sim_params
    }
    
    print(f"\nStarting Bronze Layer generation...")
    
    try:
        run_bronze_generation(counts=counts, output_path=config.BRONZE_PATH)
    except Exception as e:
        print(f"\nAn error occurred during Bronze generation: {e}")
        import traceback
        traceback.print_exc()

def handle_process_silver():
    """Handler for Step 2: Process Silver Layer."""
    print("\nStarting Step 2 (Silver) processing...")
    spark = None
    try:
        spark = init_spark()
        run_silver_processing(
            spark, 
            config.BRONZE_PATH, 
            config.SILVER_PATH, 
            config.DB_PATH
        )
    except Exception as e:
        print(f"\nAn error occurred during Silver processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            print("└── Spark Session stopped.")

def handle_aggregate_gold():
    """Handler for Step 3: Aggregate Gold Layer."""
    print("\nStarting Step 3 (Gold) aggregation...")
    try:
        run_gold_aggregation(config.DB_PATH)
    except Exception as e:
        print(f"\nAn error occurred during Gold aggregation: {e}")
        import traceback
        traceback.print_exc()

def handle_run_all():
    """Handler for running the full pipeline."""
    print("--- STARTING FULL PIPELINE ---")
    
    sim_weeks = get_week_count()
    sim_params = get_simulation_params()
    counts = {
        'weeks': sim_weeks,
        'params': sim_params
    }
    
    spark = None
    try:
        print(f"\n--- STEP 1: GENERATE BRONZE LAYER ---")
        run_bronze_generation(counts=counts, output_path=config.BRONZE_PATH)
        
        print(f"\n--- STEP 2: PROCESS SILVER LAYER ---")
        spark = init_spark()
        run_silver_processing(
            spark, 
            config.BRONZE_PATH, 
            config.SILVER_PATH, 
            config.DB_PATH
        )
        
        print("\n--- STEP 3: AGGREGATE GOLD LAYER ---")
        run_gold_aggregation(config.DB_PATH)
        
        print("\n--- FULL PIPELINE COMPLETE ---")
        
    except Exception as e:
        print(f"\nA fatal error occurred during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()
            print("└── Spark Session stopped.")

def handle_clean_data():
    """Handler for deleting all generated data."""
    print("\n--- WARNING: This will delete all generated data! ---")
    confirm = input("  > Are you sure you want to delete all data in data/bronze, data/silver, and data/analytics.db? [y/N]: ")
    if confirm.lower() != 'y':
        print("  └── Aborted. No data was deleted.")
        return
    
    print("├── Deleting data...")
    paths_to_delete = [config.BRONZE_PATH, config.SILVER_PATH]
    files_to_delete = [config.DB_PATH]

    for path in paths_to_delete:
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
                print(f"│   ├── Deleted directory: {path}")
            else:
                print(f"│   ├── Directory not found, skipped: {path}")
        except Exception as e:
            print(f"│   ├── ERROR deleting {path}: {e}")

    for file in files_to_delete:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"│   ├── Deleted file: {file}")
            else:
                print(f"│   ├── File not found, skipped: {file}")
        except Exception as e:
            print(f"│   ├── ERROR deleting {file}: {e}")
    
    print("└── Cleaning complete.")

def handle_view_gold():
    """Connects to DuckDB and queries the Gold Layer tables."""
    print("\n--- VIEW GOLD LAYER REPORTS ---")
    db_file = config.DB_PATH
    
    if not db_file.exists():
        print(f"└── ERROR: Database file not found at {db_file}")
        print("    Please run Steps 1, 2, and 3 first.")
        return

    try:
        con = duckdb.connect(database=str(db_file), read_only=True)
    except Exception as e:
        print(f"└── ERROR: Could not connect to DuckDB: {e}")
        return

    gold_tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'dm_%'").df()
    
    if gold_tables_df.empty:
        print(f"└── ERROR: No Gold Layer tables (dm_...) found in {db_file}")
        print("    Please run Step 3 (Aggregate Gold) first.")
        con.close()
        return

    report_name_mapping = {
        "dm_faction_economy": "Faction Economy Summary",
        "dm_resource_price_history": "Resource Price History (Drill-Down)",
        "dm_top_vip_customers": "Top VIP Customer Report",
        "dm_artifact_sales_summary": "Artifact Sales Summary"
    }
    
    report_list = []
    for tech_name in gold_tables_df['table_name'].tolist():
        friendly_name = report_name_mapping.get(tech_name, tech_name)
        report_list.append((tech_name, friendly_name))

    while True:
        print("\n├── Available Gold Layer Reports:")
        for i, (tech_name, friendly_name) in enumerate(report_list):
            print(f"  {i+1}. {friendly_name}")
        print("  Q. Quit to main menu")
        
        choice = input("  > Which report to view? [1-N, Q]: ")
        
        if choice.lower() == 'q':
            con.close()
            print("└── Closing report viewer.")
            break
            
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(report_list):
                table_name, friendly_name = report_list[idx]
                
                if table_name == "dm_resource_price_history":
                    print(f"\n--- Drill-down: {friendly_name} ---")
                    try:
                        resource_df = con.execute(f"SELECT DISTINCT \"Resource Name\" FROM {table_name} ORDER BY 1").df()
                        resource_list = resource_df['Resource Name'].tolist()
                        
                        if not resource_list:
                            print("  No resources found in the report.")
                            continue

                        print("  Please select a resource to view its history:")
                        for i, resource in enumerate(resource_list):
                            print(f"    {i+1}. {resource}")
                        print("    B. Back to reports")
                        
                        sub_choice = input("    > Choice: ")
                        
                        if sub_choice.lower() == 'b':
                            continue 

                        sub_idx = int(sub_choice) - 1
                        if 0 <= sub_idx < len(resource_list):
                            selected_resource = resource_list[sub_idx]
                            
                            print(f"\n--- Report: Price History for {selected_resource} ---")
                            query = f"SELECT * FROM {table_name} WHERE \"Resource Name\" = ? ORDER BY \"Year\", \"Month\""
                            df = con.execute(query, [selected_resource]).df()
                            print(df.to_string())
                        else:
                            print("  Error: Invalid resource choice.")
                            
                    except (ValueError, duckdb.Error) as e:
                        print(f"  Error during drill-down: {e}")
                
                else:
                    print(f"\n--- Report: {friendly_name} ---") 
                    df = con.execute(f"SELECT * FROM {table_name}").df()
                    print(df.to_string())
                
            else:
                print(f"  Error: Invalid choice. Please enter a number between 1 and {len(report_list)}.")
        except ValueError:
            print("  Error: Invalid input. Please enter a number or 'Q'.")
        except duckdb.Error as e:
            print(f"  An error occurred querying the table: {e}")
            con.close()
            break

def main_menu():
    """Displays the main CLI menu and handles user input."""
    print("========================================")
    print("   Heroes 3 Marketplace ETL Pipeline    ")
    print("========================================")
    
    while True:
        print("\nSelect an option:")
        print("  1. Step 1: Generate Bronze Layer")
        print("  2. Step 2: Process Silver Layer")
        print("  3. Step 3: Aggregate Gold Layer")
        print("  4. Run Full Pipeline (Steps 1-3)")
        print("  5. View Gold Layer Reports")
        print("  6. Clean All Data")
        print("  7. Exit")
        
        choice = input("Choice [1-7]: ")
        
        if choice == '1':
            handle_generate_bronze()
        elif choice == '2':
            handle_process_silver()
        elif choice == '3':
            handle_aggregate_gold()
        elif choice == '4':
            handle_run_all()
        elif choice == '5':
            handle_view_gold()
        elif choice == '6':
            handle_clean_data()
        elif choice == '7':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-7.")

if __name__ == "__main__":
    main_menu()