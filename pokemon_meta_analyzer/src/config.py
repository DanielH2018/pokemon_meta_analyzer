"""Configuration settings for Pokémon Meta Analyzer."""

from datetime import datetime

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
GEMINI_API_KEY = "AIzaSyBggEeoQFMVZj1NwJLnEVwwcNP1DfYylPI"
