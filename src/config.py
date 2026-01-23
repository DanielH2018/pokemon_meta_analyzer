"""Configuration settings for Pokémon Meta Analyzer."""

import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Settings
START_DATE = datetime.strptime("2025-11-29", "%Y-%m-%d")
PLAYER_COUNT_MINIMUM = 50
PLACING_PERCENTAGE = 0.50
PLATFORM = "all"
GAME = "PTCG"
DIVISION = ["SR", "MA"]
DATA_FOLDER = "pokemon_data"
TIME_SLICE_DAYS = 6

# Secrets (Best practice: Load these from os.environ in the future)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
