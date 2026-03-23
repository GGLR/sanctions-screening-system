"""
Sanctions List Screening System Configuration - GitHub Version
"""

import os
from pathlib import Path

# Base paths - use github folder
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Database configuration - github version uses local db in same folder
DATABASE_PATH = BASE_DIR / "sanctions.db"

# Fuzzy matching configuration
FUZZY_MATCH_THRESHOLD = 70  # Minimum score for a match (0-100) - reduced for more flexibility
FUZZY_ALGORITHM = "rapidfuzz"  # or "fuzzywuzzy"

# Match scoring weights
WEIGHTS = {
    "name_exact": 100,
    "name_similarity": 80,
    "dob_match": 15,
    "nationality_match": 10,
    "id_match": 20,
}

# Risk level thresholds
RISK_LEVELS = {
    "high": 85,      # Score >= 85
    "medium": 70,    # Score >= 70
    "low": 0,        # Score < 70
}

# Sanctions list URLs (configurable)
SANCTIONS_URLS = {
    "MOHA_MALAYSIA": "https://www.moha.gov.my/utama/images/Terkini/SENARAI_KDN_2026_BI_1.xml",
    "UN_LIST": "https://www.un.org/sc/suborg/sites/www.un.org.sc.suborg/files/consolidated.xml",
}

# Local XML file paths (for manual refresh from local files)
LOCAL_XML_FILES = {
    "MOHA_MALAYSIA": BASE_DIR / "moha_sanctions_list.xml",
    "UN_LIST": BASE_DIR / "un_sanctions_list.xml",
}

# Auto-update settings
AUTO_UPDATE_ENABLED = True
AUTO_UPDATE_INTERVAL_HOURS = 24

# Request timeout for fetching URLs (seconds)
REQUEST_TIMEOUT = 30

# XML parsing settings
XML_ENCODING = "utf-8"

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"