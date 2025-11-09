import json
import random
import pandas as pd
from itertools import count
import time
from collections import defaultdict
import os
from pathlib import Path

from config import (
    DEFINITIONS_PATH, 
    TECHNICAL_START_DATE, TECHNICAL_END_DATE, 
    LORE_START_YEAR, DEFAULT_CHUNK_SIZE
)

random.seed(42)

key_gens = {
    'faction': count(100),
    'product_category': count(200),
    'product': count(1000),
    'customer': count(5000),
    'town': count(10000),
    'trade': count(1)
}

#################################################################### --- Helper Functions ---

def load_definitions():
    """Loads the game definitions JSON file."""
    try:
        with open(DEFINITIONS_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Definitions file not found at {DEFINITIONS_PATH}")
        return None

def generate_dim_date(start_date, end_date, lore_start_year) -> pd.DataFrame:
    """Generates the date dimension using technical dates and applying a lore year offset."""
    df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
    
    technical_year_start = pd.to_datetime(start_date).year
    year_offset = lore_start_year - technical_year_start
    
    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["year"] = df["date"].dt.year + year_offset
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    
    df["date_key"] = (
        df["year"] * 10000 +
        df["month"] * 100 +
        df["day"]
    ).astype(int)
    
    df['lore_date_str'] = df['year'].astype(str) + '-' + \
                           df['month'].astype(str).str.zfill(2) + '-' + \
                           df['day'].astype(str).str.zfill(2)
                           
    return df[['date_key', 'lore_date_str', 'day_of_week', 'month', 'year', 'is_weekend']]

def flatten_dimensions(definitions):
    """Converts the nested JSON definitions into flat DataFrames for all dimensions."""
    customers_data = []
    products_data = []
    towns_data = []
    factions_data = []
    product_categories_data = []
    
    town_prefixes = definitions['town_names']['prefixes']
    town_suffixes = definitions['town_names']['suffixes']
    
    for faction_name in definitions['factions'].keys():
        faction_key = next(key_gens['faction'])
        factions_data.append({'faction_key': faction_key, 'faction_name': faction_name})
        
        for _ in range(5):
            town_name = f"{random.choice(town_prefixes)} {random.choice(town_suffixes)}"
            towns_data.append({
                'town_key': next(key_gens['town']),
                'town_name': town_name,
                'faction_key': faction_key
            })

    df_factions = pd.DataFrame(factions_data)
    df_towns = pd.DataFrame(towns_data)
    faction_map = df_factions.set_index('faction_name')['faction_key'].to_dict()
    
    for faction_name, data in definitions['factions'].items():
        faction_key = faction_map[faction_name]
        
        for unit in data['units']:
            customers_data.append({
                'customer_key': next(key_gens['customer']),
                'customer_name': unit['name'],
                'customer_segment': 'Standard',
                'unit_tier': unit['tier'],
                'base_income': unit['cost'],
                'faction_key': faction_key
            })
        
        for hero in data['heroes']:
            customers_data.append({
                'customer_key': next(key_gens['customer']),
                'customer_name': hero['name'],
                'customer_segment': 'VIP',
                'unit_tier': 0,
                'base_income': hero['cost'],
                'faction_key': faction_key
            })
    df_customers = pd.DataFrame(customers_data)

    for cat_name, tiers in definitions['products'].items():
        for tier_level in tiers.keys():
            product_categories_data.append({
                'category_key': next(key_gens['product_category']),
                'category_name': cat_name,
                'tier_level': tier_level
            })
            
    df_product_categories = pd.DataFrame(product_categories_data)
    cat_map = df_product_categories.set_index(['category_name', 'tier_level'])['category_key'].to_dict()

    for cat_name, tiers in definitions['products'].items():
        for tier_level, items in tiers.items():
            category_key = cat_map[(cat_name, tier_level)]
            for item in items:
                products_data.append({
                    'product_key': next(key_gens['product']),
                    'product_name': item['name'],
                    'base_value_gold': item['cost'],
                    'category_key': category_key,
                    'category_name': cat_name, 
                    'tier_level': tier_level
                })
                
    df_products = pd.DataFrame(products_data)
    
    return df_customers, df_products, df_towns, df_factions, df_product_categories

#################################################################### --- Agent-Based Model Architecture ---

class Agent:
    """Represents a customer (Unit or Hero) with persistent state."""
    
    def __init__(self, customer_data: dict, sim_params: dict):
        self.key = customer_data['customer_key']
        self.name = customer_data['customer_name']
        self.segment = customer_data['customer_segment']
        self.tier = customer_data['unit_tier']
        self.base_income = customer_data['base_income']
        self.gold = 0.0
        self.inventory = defaultdict(float) 
        self.params = sim_params

    def pay_cost_of_living(self):
        """Pays maintenance and wealth tax."""
        cost_of_living = self.base_income * self.params['cost_of_living_rate']
        wealth_tax = self.gold * self.params['wealth_tax_rate']
        total_cost = cost_of_living + wealth_tax
        self.gold = max(0.0, self.gold - total_cost)

    def receive_income(self):
        """Receives weekly income."""
        self.gold += self.base_income

    def choose_product_tier_to_buy(self, product_tiers):
        """Decides which *type* of product to buy based on wealth/status."""
        weights = [0.0, 0.0, 0.0, 0.0] 
        if self.segment == 'VIP':
            weights = [0.70, 0.25, 0.045, 0.005] 
        elif self.base_income > 100:
            weights = [0.85, 0.13, 0.019, 0.001] 
        elif self.base_income > 20:
            weights = [0.95, 0.049, 0.001, 0.0]
        else:
            weights = [0.9999, 0.0, 0.0, 0.0001] 
            
        tier_name = random.choices(list(product_tiers.keys()), weights=weights, k=1)[0]
        if not product_tiers[tier_name]:
            tier_name = 'low' 
        return tier_name

    def get_quantity_to_buy(self, product):
        """Decides *how much* product to buy based on status."""
        if product['category_name'] == 'Artifacts':
            return 1
        if self.segment == 'VIP' or self.tier >= 7:
            return random.randint(100, 1000)
        elif self.tier >= 4:
            return random.randint(10, 50)
        else:
            return round(random.uniform(0.1, 5.0), 2)

    def choose_resource_to_sell(self, products_dict):
        """Decides *what* and *how much* to sell from inventory."""
        resources_owned = [
            pk for pk, qty in self.inventory.items() 
            if products_dict[pk]['category_name'] == 'Resources' and qty > 0.1
        ]
        if not resources_owned:
            return None, 0
        
        product_key_to_sell = random.choice(resources_owned)
        sell_quantity = round(self.inventory[product_key_to_sell] * random.uniform(0.1, 0.5), 2)
        
        if sell_quantity < 0.1:
            return None, 0
        return product_key_to_sell, sell_quantity

    def make_decision(self):
        """The main "brain" of the agent. Decides to BUY, SELL, or HOLD."""
        if self.gold < (self.base_income * 2) and len(self.inventory) > 0:
             buy = self.params['unit_buy_chance']
             sell = self.params['unit_sell_chance']
        else: 
             buy = self.params['vip_buy_chance']
             sell = self.params['vip_sell_chance']
        
        hold = 1.0 - buy - sell
        action = random.choices(['BUY', 'SELL', 'HOLD'], weights=[buy, sell, hold], k=1)[0]
        return action

class Market:
    """Represents the global market, managing prices, stock, and transactions."""
    
    def __init__(self, df_products):
        self.products_dict = df_products.set_index('product_key').to_dict('index')
        self.product_tiers = self._get_product_tiers(df_products)
        self.market_prices = df_products.set_index('product_key')['base_value_gold'].to_dict()
        
        self.global_artifact_pool = {}
        for tier, items in self.product_tiers.items():
            if tier == 'low': continue
            for item in items:
                pk = item['product_key']
                if tier == 'relic': self.global_artifact_pool[pk] = 1 
                elif tier == 'high': self.global_artifact_pool[pk] = 5 
                elif tier == 'mid': self.global_artifact_pool[pk] = 20 
        
        self.weekly_demand = defaultdict(float)
        self.weekly_supply = defaultdict(float)
        self.current_chunk_transactions = [] 
        self.town_keys_list = [] 

    def _get_product_tiers(self, products_df):
        """Private method to create product tiers."""
        products = products_df.to_dict('records')
        return {
            'low': [p for p in products if p['category_name'] == 'Resources' and p['product_name'] != 'Gold'],
            'mid': [p for p in products if p['tier_level'] in ['Treasure', 'Minor']],
            'high': [p for p in products if p['tier_level'] == 'Major'],
            'relic': [p for p in products if p['tier_level'] == 'Relic']
        }

    def execute_buy_transaction(self, agent, week_date_key):
        """Attempts to execute a BUY action for an agent."""
        tier_to_buy = agent.choose_product_tier_to_buy(self.product_tiers)
        
        available_products = [
            p for p in self.product_tiers[tier_to_buy] 
            if self.market_prices[p['product_key']] <= agent.gold
        ]
        
        if not available_products:
            available_products = [
                p for p in self.product_tiers['low'] 
                if self.market_prices[p['product_key']] <= agent.gold
            ]
            if not available_products:
                return 

        product = random.choice(available_products)
        product_key = product['product_key']

        if product['category_name'] == 'Artifacts':
            if agent.inventory[product_key] > 0:
                return 
            if self.global_artifact_pool[product_key] <= 0:
                return 

        quantity = agent.get_quantity_to_buy(product)
        current_price = self.market_prices[product_key]
        total_cost = round(current_price * quantity, 2)

        if total_cost > agent.gold:
            if product['category_name'] == 'Artifacts':
                return 
            else:
                min_cost = current_price * 0.1
                if agent.gold < min_cost:
                    return 
                quantity = round(agent.gold / current_price, 2)
                total_cost = agent.gold
                if quantity < 0.1:
                     return

        agent.gold -= total_cost
        agent.inventory[product_key] += quantity
        
        if product['category_name'] == 'Artifacts':
            self.global_artifact_pool[product_key] -= 1 

        self.weekly_demand[product_key] += quantity 
        
        self.current_chunk_transactions.append({
            'trade_key': next(key_gens['trade']),
            'date_key': week_date_key,
            'transaction_type': 'BUY',
            'customer_key': agent.key,
            'product_key': product_key,
            'town_key': random.choice(self.town_keys_list),
            'quantity': quantity,
            'gold_per_unit': current_price,
            'gold_total': total_cost,
            'current_gold_balance': agent.gold
        })

    def execute_sell_transaction(self, agent, week_date_key):
        """Attempts to execute a SELL action for an agent."""
        product_key, quantity = agent.choose_resource_to_sell(self.products_dict)
        
        if not product_key or quantity <= 0:
            return 

        current_price = self.market_prices[product_key]
        total_gain = round(current_price * quantity, 2)
        
        agent.gold += total_gain
        agent.inventory[product_key] -= quantity
        self.weekly_supply[product_key] += quantity 

        self.current_chunk_transactions.append({
            'trade_key': next(key_gens['trade']),
            'date_key': week_date_key,
            'transaction_type': 'SELL',
            'customer_key': agent.key,
            'product_key': product_key,
            'town_key': random.choice(self.town_keys_list),
            'quantity': quantity,
            'gold_per_unit': current_price,
            'gold_total': total_gain,
            'current_gold_balance': agent.gold
        })

    def update_market_prices(self):
        """Updates market prices based on weekly supply and demand."""
        for product_key in self.market_prices.keys():
            if self.products_dict[product_key]['category_name'] == 'Artifacts':
                continue 

            net_demand = self.weekly_demand[product_key] - self.weekly_supply[product_key]
            base_price = self.products_dict[product_key]['base_value_gold']
            current_price = self.market_prices[product_key]

            if net_demand > 0: 
                change_factor = 1.0 + (net_demand / 10000.0)
                current_price *= change_factor
            elif net_demand < 0:
                change_factor = 1.0 - (abs(net_demand) / 10000.0)
                current_price *= change_factor
            
            current_price = (current_price * 0.95) + (base_price * 0.05)
            self.market_prices[product_key] = max(base_price * 0.1, min(current_price, base_price * 10))
                
        self.weekly_demand.clear()
        self.weekly_supply.clear()

    def get_and_clear_chunk(self):
        """Returns the current transaction chunk and clears it."""
        chunk = self.current_chunk_transactions
        self.current_chunk_transactions = []
        return chunk

#################################################################### --- Main Execution ---

def run_bronze_generation(counts: dict, output_path: Path):
    """
    The main orchestration function. Generates all dimension tables
    and simulates the fact table in chunks.
    """
    start_time = time.time()
    print("\n--- STEP 1: GENERATE BRONZE LAYER ---")
    
    sim_weeks = counts.get('weeks', 104) 
    sim_params = counts.get('params', {
        'cost_of_living_rate': 0.5, 'wealth_tax_rate': 0.05,
        'vip_buy_chance': 0.5, 'vip_sell_chance': 0.2,
        'unit_buy_chance': 0.2, 'unit_sell_chance': 0.6,
    })
    
    technical_start = pd.to_datetime(TECHNICAL_START_DATE)
    technical_end = pd.to_datetime(TECHNICAL_END_DATE)
    
    print(f"├── Loading definitions from {DEFINITIONS_PATH}...")
    definitions = load_definitions()
    if not definitions:
        return

    print("├── Flattening dimensions...")
    df_date = generate_dim_date(technical_start, technical_end, LORE_START_YEAR)
    df_customers, df_products, df_towns, df_factions, df_product_categories = flatten_dimensions(definitions)
    
    print("├── Initializing Market and Agents...")
    market = Market(df_products)
    market.town_keys_list = df_towns['town_key'].tolist()

    agents_dict = {
        row['customer_key']: Agent(row, sim_params) 
        for row in df_customers.to_dict('records')
    }
    
    all_date_keys = df_date['date_key'].tolist()
    date_key_to_partition = df_date.set_index('date_key')[['year', 'month']].to_dict('index')
    
    print(f"├── Saving 6 dimension tables to {output_path}...")
    os.makedirs(output_path, exist_ok=True)
    
    df_factions.to_parquet(output_path / "dim_faction.parquet", index=False, engine='pyarrow')
    df_product_categories.to_parquet(output_path / "dim_product_category.parquet", index=False, engine='pyarrow')
    df_date.to_parquet(output_path / "dim_date.parquet", index=False, engine='pyarrow')
    df_towns.to_parquet(output_path / "dim_town.parquet", index=False, engine='pyarrow')
    
    cols_to_drop = ['category_name', 'tier_level']
    df_products_clean = df_products.drop(columns=[col for col in cols_to_drop if col in df_products.columns])
    df_products_clean.to_parquet(output_path / "dim_product.parquet", index=False, engine='pyarrow')
    
    df_customers.to_parquet(output_path / "dim_customer.parquet", index=False, engine='pyarrow')
    print(f"│   └── Dimensions saved.")

    fact_sales_dir = output_path / "fact_sales"
    os.makedirs(fact_sales_dir, exist_ok=True)
    total_transactions = 0
    
    print(f"├── Starting simulation for {sim_weeks} weeks (chunk size: {DEFAULT_CHUNK_SIZE} weeks)...")
    
    for week_idx in range(sim_weeks):
        if (week_idx + 1) % (sim_weeks // 10 if sim_weeks >= 10 else 1) == 0:
             print(f"│   ...Week {week_idx+1}/{sim_weeks}")
            
        for agent in agents_dict.values():
            agent.pay_cost_of_living()
            agent.receive_income()
            action = agent.make_decision()
            trans_date_key = random.choice(all_date_keys)

            if action == 'BUY':
                market.execute_buy_transaction(agent, trans_date_key)
            elif action == 'SELL':
                market.execute_sell_transaction(agent, trans_date_key)

        market.update_market_prices()
        
        # --- Incremental Parquet Write (Chunking) ---
        if (week_idx + 1) % DEFAULT_CHUNK_SIZE == 0 or (week_idx + 1) == sim_weeks:
            chunk_data = market.get_and_clear_chunk()
            
            if not chunk_data:
                continue 
                
            df_chunk = pd.DataFrame(chunk_data)
            
            def get_partition_info(date_key, part):
                info = date_key_to_partition.get(date_key)
                if info:
                    return info.get(part)
                return int(str(date_key)[:4]) if part == 'year' else int(str(date_key)[4:6])

            df_chunk['year'] = df_chunk['date_key'].apply(lambda x: get_partition_info(x, 'year'))
            df_chunk['month'] = df_chunk['date_key'].apply(lambda x: get_partition_info(x, 'month'))

            try:
                df_chunk.to_parquet(
                    fact_sales_dir,
                    index=False,
                    engine='pyarrow',
                    partition_cols=['year', 'month']
                )
            except Exception as e:
                print(f"  ERROR writing chunk to Parquet: {e}")
            
            total_transactions += len(df_chunk)
            print(f"│   └── Saved chunk {week_idx // DEFAULT_CHUNK_SIZE + 1} ({len(df_chunk)} rows) to Parquet.")

    end_time = time.time()
    print(f"└── Simulation finished in {time.time() - start_time:.2f} seconds.")
    print(f"\n    Total transactions generated: {total_transactions}")
    print("\n--- STEP 1: BRONZE LAYER GENERATION COMPLETE ---")