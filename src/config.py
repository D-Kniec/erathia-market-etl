from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DEFINITIONS_PATH = BASE_DIR / "data_definitions" / "game_definitions.json"
BRONZE_PATH = BASE_DIR / "data" / "bronze"
SILVER_PATH = BASE_DIR / "data" / "silver"
DB_PATH = BASE_DIR / "data" / "analytics.db"
TECHNICAL_START_DATE = '2000-01-01'
TECHNICAL_END_DATE = '2001-12-31' 
LORE_START_YEAR = 1168
DEFAULT_COST_OF_LIVING = 0.5
DEFAULT_WEALTH_TAX = 0.05
CHUNK_SIZE_IN_WEEKS = 100
DEFAULT_CHUNK_SIZE=100