# ⚔️ Erathia Market ETL ⚔️

Hi there! This is a complete, end-to-end data engineering portfolio project, built for fun.

Instead of generating boring, random sales data, this pipeline simulates the **entire market economy of the Heroes of Might & Magic 3 universe**.

The goal was to see if a Pikeman could ever actually afford a Grail. (Spoiler: with a 5% weekly wealth tax, not a chance.)

## 🏛️ Architecture (Medallion)

The pipeline follows the classic Bronze -> Silver -> Gold medallion architecture.

### 🥉 Bronze Layer
This is where the magic happens. I built a stateful **Agent-Based Model (ABM)** simulation in Pandas.

* **Agents** (Heroes and Units) have a weekly `base_income`.
* They pay weekly **Cost of Living** *and* a **Wealth Tax** (our inflation) to prevent absurd multi-million gold balances.
* They make decisions: `BUY`, `SELL`, or `HOLD`. Poor agents (like a Gnoll) are more likely to sell, while rich agents (Heroes) are more likely to buy.
* **The Market** has **dynamic resource prices** (changing weekly based on supply and demand) and a **global, limited artifact pool** (e.g., only 1 Grail exists for the entire simulation).
* All 7 raw tables (dims and facts) are saved incrementally (in chunks) as partitioned **Parquet** files.

### 🥈 Silver Layer
* **PySpark** reads all the raw, partitioned Parquet files from the Bronze layer.
* It cleans the data (deduplication, handling errors).
* It enriches the data (e.g., adding an `is_hero` column).
* It loads the final, clean tables into a **DuckDB** analytical database (`analytics.db`). This is our single source of truth.

### 🥇 Gold Layer
* A final script connects to the Silver DuckDB.
* It runs analytical SQL queries to create aggregated "data marts" for "Queen Catherine."
* It creates report-ready tables like `dm_faction_economy` and `dm_artifact_sales_summary`.

## 🛠️ Tech Stack

* **Python 3.10+**
* **Pandas:** To power the agent-based simulation engine (Bronze).
* **PySpark:** For large-scale data processing and transformation (Bronze -> Silver).
* **DuckDB:** As our fast, analytical database (Silver & Gold).
* **PyArrow & Parquet:** For efficient, columnar storage in our data lake.
* **CLI:** The entire pipeline is wrapped in a user-friendly Command Line Interface.

## 🚀 How to Run

1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/D-Kniec/erathia-market-etl.git](https://github.com/D-Kniec/erathia-market-etl.git)
    cd erathia-market-etl
    ```

2.  **Create a venv and install requirements:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Run the CLI:**
    ```bash
    python main.py
    ```

4.  **Follow the menu prompts:**
    * Select **Option 1** to configure your simulation (e.g., `10000` weeks) and generate the Bronze Parquet files.
    * Select **Option 2** to have PySpark process the data into DuckDB.
    * Select **Option 3** to aggregate the Gold Layer data marts.
    * Select **Option 5** to instantly view the final reports in your terminal.
    * (Or select **Option 4** to run all steps at once).

## 📊 Example Report (Gold Layer)

After running the pipeline, you can immediately query the data marts you just built.
