"""Analysis module for fetching and processing Pokémon TCG meta data."""

import os
import time
from datetime import datetime

import pandas as pd
import requests

from src import config
from src.logger_utils import add_context, get_logger

# region Data Fetching


def fetch_matchup_data(
    deck_list: list[str],
    start_date: str,
    end_date: str = datetime.now().strftime("%Y-%m-%d"),
) -> pd.DataFrame:
    """Fetches matchup data from TrainerHill for a given date range.

    Args:
        deck_list: List of deck slugs to fetch data for.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format (default is today).

    Returns:
        A raw pandas DataFrame or None on error.
    """
    logger = get_logger()
    logger.info(f"Calling API to fetch data from {start_date} to {end_date}...")

    url = "https://www.trainerhill.com/_dash-update-component"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # Payload now correctly uses the function's date arguments
    payload = {
        "output": "meta-matchup-data-store.data",
        "outputs": {"id": "meta-matchup-data-store", "property": "data"},
        "inputs": [
            {
                "id": "meta-tour-store",
                "property": "data",
                "value": {
                    "players": config.PLAYER_COUNT_MINIMUM,
                    "start_date": start_date,
                    "end_date": end_date,
                    "platform": config.PLATFORM,
                    "game": config.GAME,
                    "division": config.DIVISION,
                },
            },
            {
                "id": "meta-archetype-select",
                "property": "value",
                "value": deck_list,
            },
            {
                "id": "meta-placing",
                "property": "value",
                "value": config.PLACING_PERCENTAGE,
            },
            {
                "id": "meta-result-rate",
                "property": "value",
                "value": "ties_count_as_third_win",
            },
        ],
        "changedPropIds": ["meta-archetype-select.value"],
        "parsedChangedPropsIds": ["meta-archetype-select.value"],
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()

        matchup_list = response_data["response"]["meta-matchup-data-store"][
            "data"
        ]

        if isinstance(matchup_list, list) and matchup_list:
            logger.info("API call successful!")
            return pd.DataFrame(matchup_list)
        else:
            logger.warning("No data returned from API for this period.")
            return pd.DataFrame()  # Return empty DataFrame

    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred during API request: {e}")
        return pd.DataFrame()
    except KeyError:
        logger.error("Could not find expected data in API response.")
        return pd.DataFrame()


def get_deck_slugs(
    players: int = config.PLAYER_COUNT_MINIMUM,
    start_date: str = config.START_DATE.strftime("%Y-%m-%d"),
    end_date: str = datetime.now().strftime("%Y-%m-%d"),
    platform: str = config.PLATFORM,
    game: str = config.GAME,
    division: list[str] = config.DIVISION,
) -> list[str]:
    """Perform the deck-select POST and return a list of deck titles.

    Args:
        players: Minimum players value (default PLAYER_COUNT_MINIMUM)
        start_date: YYYY-MM-DD start date
        end_date: YYYY-MM-DD end date
        platform: platform filter (default PLATFORM)
        game: game filter (default GAME)
        division: list of divisions, defaults to DIVISION

    Returns:
        A list of deck titles
    """
    logger = get_logger()
    logger.info(f"Calling deck-select API for {start_date} to {end_date})")

    url = "https://www.trainerhill.com/_dash-update-component"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "output": "..meta-archetype-select.options...meta-archetype-store.data..",
        "outputs": [
            {"id": "meta-archetype-select", "property": "options"},
            {"id": "meta-archetype-store", "property": "data"},
        ],
        "inputs": [
            {
                "id": "meta-tour-store",
                "property": "data",
                "value": {
                    "players": config.PLAYER_COUNT_MINIMUM,
                    "start_date": start_date,
                    "end_date": end_date,
                    "platform": config.PLATFORM,
                    "game": config.GAME,
                    "division": config.DIVISION,
                },
            }
        ],
        "changedPropIds": [],
        "parsedChangedPropsIds": [],
    }
    logger.debug(f"Payload: {payload}")

    op_start = time.time()
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        response_data = resp.json()

        # logger.debug(f"Response: {response_data}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error performing deck-select POST: {e}")
        add_context(deck_slugs_fetch_failed=True, deck_slugs_error=str(e))
        return []

    try:
        deck_children = (
            response_data.get("response", {})
            .get("meta-archetype-store", {})
            .get("data", {})
        )
        deck_ids = []
        if isinstance(deck_children, list):
            for child in deck_children[:20]:  # Limit to first 20 entries
                try:
                    deck_id = child.get("id", {})
                except (KeyError, TypeError, IndexError):
                    deck_id = None
                if deck_id is not None:
                    deck_ids.append(deck_id)
        else:
            logger.warning("Unexpected structure for deck ids; expected list.")
        logger.debug(f"Deck IDs: {deck_ids}")
        op_duration = (time.time() - op_start) * 1000
        add_context(
            deck_ids_count=len(deck_ids),
            deck_ids_fetch_duration_ms=round(op_duration, 2),
        )

        return deck_ids

    except (KeyError, ValueError) as e:
        logger.error(f"Could not parse deck-select response: {e}")
        add_context(deck_slugs_parse_failed=True, deck_slugs_parse_error=str(e))
        return []


def get_time_slice_data(
    time_slice_start,
    time_slice_end,
    is_current_time_slice,
    deck_list,
    folder="pokemon_data",
) -> pd.DataFrame:
    """Orchestrates fetching data for a time slice.

    If it's the current time slice, it re-fetches data. Otherwise, it uses the cache.

    Args:
        time_slice_start: Start date of the time slice in YYYY-MM-DD format.
        time_slice_end: End date of the time slice in YYYY-MM-DD format.
        is_current_time_slice: Boolean indicating if this is the current time slice.
        deck_list: List of deck slugs to fetch data for.
        folder: Folder to save and load data from.

    Returns:
        A pandas DataFrame containing the fetched or cached data.
    """
    logger = get_logger()
    filename = f"data_{time_slice_start}_to_{time_slice_end}.csv"
    filepath = os.path.join(folder, filename)

    if is_current_time_slice and os.path.exists(filepath):
        logger.info(
            f"Current time slice detected. Removing stale file to re-fetch: {filename}"
        )
        os.remove(filepath)

    if os.path.exists(filepath):
        logger.info(
            f"Found local file for past time slice: {filename}. Loading from disk."
        )
        df = pd.read_csv(filepath)
        add_context(**{f"time_slice_data_{time_slice_start}_from_cache": True})
        return df
    else:
        raw_df = fetch_matchup_data(deck_list, time_slice_start, time_slice_end)
        if raw_df is not None and not raw_df.empty:
            raw_df.to_csv(filepath, index=False)
            logger.info(f"Saved data to {filepath}")
            add_context(
                **{
                    f"time_slice_data_{time_slice_start}_rows_fetched": len(
                        raw_df
                    )
                }
            )
        return raw_df


# endregion
