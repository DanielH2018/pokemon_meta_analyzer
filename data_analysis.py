import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from google import genai

# Constants

START_DATE = datetime.strptime("2025-11-29", "%Y-%m-%d")
PLAYER_COUNT_MINIMUM = 50
# Top Percentage Place e.g. 0.25 -> 25%
PLACING_PERCENTAGE = 0.50
PLATFORM = "all"  # inperson
GAME = "PTCG"
DIVISION = ["SR", "MA"]
DATA_FOLDER = "pokemon_data"
# Number of days to process at a time - 1
TIME_SLICE = 6
GEMINI_API_KEY = "AIzaSyBggEeoQFMVZj1NwJLnEVwwcNP1DfYylPI"


# region Logging Setup

# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s\n",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class WideEvent:
    """
    A simple 'Flight Recorder'. It gathers data as the script runs
    and prints one JSON summary at the end.
    """

    def __init__(self, script_name):
        self.data = {
            "script": script_name,
            "timestamp": time.time(),
            "status": "success",
        }
        self.start_time = time.time()

    def add(self, **kwargs):
        """Add any info you want to track."""
        self.data.update(kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # 1. Calculate how long the script took
        duration = (time.time() - self.start_time) * 1000
        self.data["duration_ms"] = round(duration, 2)

        # 2. If it crashed, capture why
        if exc_type:
            self.data["status"] = "failed"
            self.data["error"] = str(exc_value)
            self.data["error_type"] = exc_type.__name__

        # 3. Print the single wide event
        print(json.dumps(self.data, indent=4))

        # Allow the crash to actually stop the script (don't suppress it)
        return False


# endregion


# ==============================================================================
# PART 1: DATA FETCHING AND CACHING
# ==============================================================================

# region Data Fetching


def fetch_matchup_data(
    deck_list: list[str],
    start_date: str,
    end_date: str = datetime.now().strftime("%Y-%m-%d"),
    event: WideEvent = None,
) -> pd.DataFrame | None:
    """
    Fetches matchup data from TrainerHill for a given date range.
    Returns a raw pandas DataFrame.
    """
    logging.info(f"Calling API to fetch data from {start_date} to {end_date}...")

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
                    "players": PLAYER_COUNT_MINIMUM,
                    "start_date": start_date,
                    "end_date": end_date,
                    "platform": PLATFORM,
                    "game": GAME,
                    "division": DIVISION,
                },
            },
            {
                "id": "meta-archetype-select",
                "property": "value",
                "value": deck_list,
            },
            {"id": "meta-placing", "property": "value", "value": PLACING_PERCENTAGE},
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

        matchup_list = response_data["response"]["meta-matchup-data-store"]["data"]

        if isinstance(matchup_list, list) and matchup_list:
            logging.info("API call successful!")
            return pd.DataFrame(matchup_list)
        else:
            logging.warning("No data returned from API for this period.")
            return None  # Return empty DataFrame

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred during API request: {e}")
        return None
    except KeyError:
        logging.error("Could not find expected data in API response.")
        return None


def get_deck_slugs(
    players: int = PLAYER_COUNT_MINIMUM,
    start_date: str = START_DATE.strftime("%Y-%m-%d"),
    end_date: str = datetime.now().strftime("%Y-%m-%d"),
    platform: str = PLATFORM,
    game: str = GAME,
    division: list[str] = DIVISION,
    event=None,
) -> list[str]:
    """Perform the deck-select POST and return a list of deck titles.

    Parameters:
        players: Minimum players value (default PLAYER_COUNT_MINIMUM)
        start_date: YYYY-MM-DD start date
        end_date: YYYY-MM-DD end date
        platform: platform filter (default PLATFORM)
        game: game filter (default GAME)
        division: list of divisions, defaults to DIVISION

    Returns:
        A list of deck titles
    """

    logging.info(f"Calling deck-select API for {start_date} to {end_date})")

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
                    "players": PLAYER_COUNT_MINIMUM,
                    "start_date": start_date,
                    "end_date": end_date,
                    "platform": PLATFORM,
                    "game": GAME,
                    "division": division,
                },
            }
        ],
        "changedPropIds": [],
        "parsedChangedPropsIds": [],
    }
    logging.debug(f"Payload: {payload}")

    op_start = time.time()
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        response_data = resp.json()

        # logging.debug(f"Response: {response_data}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error performing deck-select POST: {e}")
        if event:
            event.add(deck_slugs_fetch_failed=True, deck_slugs_error=str(e))
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
            logging.warning("Unexpected structure for deck ids; expected list.")
        logging.debug(f"Deck IDs: {deck_ids}")
        op_duration = (time.time() - op_start) * 1000
        if event:
            event.add(
                deck_ids_count=len(deck_ids),
                deck_ids_fetch_duration_ms=round(op_duration, 2),
            )

        return deck_ids

    except (KeyError, ValueError) as e:
        logging.error(f"Could not parse deck-select response: {e}")
        if event:
            event.add(deck_slugs_parse_failed=True, deck_slugs_parse_error=str(e))
        return []


def get_time_slice_data(
    time_slice_start,
    time_slice_end,
    is_current_time_slice,
    deck_list,
    folder="pokemon_data",
    event=None,
):
    """
    Orchestrates fetching data for a time slice.
    If it's the current time slice, it re-fetches data. Otherwise, it uses the cache.
    """
    filename = f"data_{time_slice_start}_to_{time_slice_end}.csv"
    filepath = os.path.join(folder, filename)

    if is_current_time_slice and os.path.exists(filepath):
        logging.info(
            f"Current time slice detected. Removing stale file to re-fetch: {filename}"
        )
        os.remove(filepath)

    if os.path.exists(filepath):
        logging.info(
            f"Found local file for past time slice: {filename}. Loading from disk."
        )
        df = pd.read_csv(filepath)
        if event:
            event.add(**{f"time_slice_data_{time_slice_start}_from_cache": True})
        return df
    else:
        raw_df = fetch_matchup_data(deck_list, time_slice_start, time_slice_end, event)
        if raw_df is not None and not raw_df.empty:
            raw_df.to_csv(filepath, index=False)
            logging.info(f"Saved data to {filepath}")
            if event:
                event.add(
                    **{f"time_slice_data_{time_slice_start}_rows_fetched": len(raw_df)}
                )
        return raw_df


# endregion

# ==============================================================================
# PART 2: POWER RANKING ANALYSIS
# ==============================================================================

# region Power Ranking Analysis


def run_power_analysis(df, title="FINAL POWER RANKINGS", event=None):
    """
    Executes the iterative power ranking algorithm.
    Logs the result and returns the ranking table as a string.
    """
    if df.empty:
        logging.warning(f"Skipping analysis for '{title}' due to no data.")
        if event:
            event.add(power_analysis_empty_data=True)
        return None  # Return None if no data

    all_decks = pd.unique(df[["deck1", "deck2"]].values.ravel("K"))
    power_scores = pd.Series(1.0, index=all_decks)

    # --- Iterative Calculation ---
    for i in range(20):
        df["opponent_power"] = df["deck2"].map(power_scores)
        df["power_wins"] = df["wins"] * df["opponent_power"]
        df["power_total"] = df["total"] * df["opponent_power"]

        power_wins_sum = df.groupby("deck1")["power_wins"].sum()
        power_total_sum = df.groupby("deck1")["power_total"].sum()

        new_power_scores = power_wins_sum / power_total_sum
        new_power_scores = new_power_scores.reindex(all_decks).fillna(0)

        if new_power_scores.sum() == 0:
            power_scores = new_power_scores
            break
        power_scores = new_power_scores / new_power_scores.sum()
        logging.debug(f"Iteration {i + 1} complete for '{title}'.")

    # --- Final Output ---
    summary_df = pd.DataFrame(index=all_decks)
    summary_df["power_rank_score"] = (power_scores * 1000).round(2)
    summary_df = summary_df.sort_values(by="power_rank_score", ascending=False)

    table_string = summary_df.head(15).to_string()

    logging.info(f"\n{'=' * 50}\n {title}\n{'=' * 50}\n{table_string}")

    if event:
        event.add(
            power_analysis_decks_analyzed=len(all_decks), power_analysis_iterations=20
        )

    # Return the table string for external use (like the AI prompt)
    return table_string


# endregion

# ==============================================================================
# PART 3: GEMINI AI ANALYSIS
# ==============================================================================

# region AI Analysis


def gemini_analysis(final_rankings_table):
    try:
        # Configure the Gemini client with the API key from environment variables
        # The client gets the API key from the environment variable `GEMINI_API_KEY`.
        client = genai.Client(api_key=GEMINI_API_KEY)

        prompt = f"""
        You are a professional Pokémon Trading Card Game meta-game analyst.
        Based on the following power rankings data, provide a brief but insightful analysis for a competitive player.

        Your analysis should:
        1.  **Identify the Top Tier:** Name the top 3-5 decks and briefly explain why they are likely dominant based on the data.
        2.  **Mention Sleepers/Contenders:** Point out 1-2 decks outside the top tier that have potential or are showing surprising strength.
        3.  **Meta Overview:** Give a one-sentence summary of the meta's health (e.g., "The meta appears diverse with multiple viable archetypes," or "The meta is heavily centralized around Charizard-Pidgeot.").
        4.  **Formatting:** Use markdown for clarity (e.g., bolding deck names, using bullet points).

        **Power Rankings Data:**
        ---
        {final_rankings_table}
        ---

        Provide your analysis now.
        """

        logging.info("\n\n{'='*20} 🤖 GEMINI META ANALYSIS 🤖 {'='*20}")

        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
        )

        logging.info(response.text)

    except ImportError:
        logging.warning(
            "\n\nCould not perform AI analysis. Please install the Gemini client: pip install google-generativeai"
        )
    except Exception as e:
        logging.error(f"\n\nAn error occurred during Gemini analysis: {e}")


# endregion

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

# region Main

if __name__ == "__main__":
    with WideEvent("pokemon_matchup_analysis") as event:
        event.add(script_name="pokemon_matchup_analysis")
        os.makedirs(DATA_FOLDER, exist_ok=True)

        start_date_dt = START_DATE
        end_date_dt = datetime.now()
        event.add(
            date_range_start=start_date_dt.strftime("%Y-%m-%d"),
            date_range_end=end_date_dt.strftime("%Y-%m-%d"),
        )

        time_slice_start_dt = start_date_dt
        all_dfs = []

        today = datetime.now()
        aggregated_df_summed = pd.DataFrame()
        time_periods_processed = 0
        total_rows_fetched = 0

        while time_slice_start_dt <= end_date_dt:
            time_slice_end_dt = time_slice_start_dt + timedelta(days=6)

            time_splice_start_str = time_slice_start_dt.strftime("%Y-%m-%d")
            time_splice_end_str = time_slice_end_dt.strftime("%Y-%m-%d")

            logging.info(
                f"{'=' * 25} PROCESSING TIME: {time_splice_start_str} to {time_splice_end_str} {'=' * 25}"
            )

            is_ongoing_time = time_slice_start_dt <= today <= time_slice_end_dt

            deck_list = get_deck_slugs(
                start_date=time_splice_start_str,
                end_date=time_splice_end_str,
                event=event,
            )

            raw_df = get_time_slice_data(
                time_splice_start_str,
                time_splice_end_str,
                is_ongoing_time,
                deck_list,
                DATA_FOLDER,
                event=event,
            )

            if raw_df is not None and not raw_df.empty:
                all_dfs.append(raw_df)
                total_rows_fetched += len(raw_df)
                time_periods_processed += 1
                run_power_analysis(
                    raw_df.copy(),
                    f"RANKINGS FOR TIME SLICE: {time_splice_start_str} to {time_splice_end_str}",
                    event=event,
                )

                aggregated_df = pd.concat(all_dfs, ignore_index=True)
                aggregated_df_summed = (
                    aggregated_df.groupby(["deck1", "deck2"]).sum().reset_index()
                )

            time_slice_start_dt += timedelta(days=7)

        # Final aggregate analysis for the entire period
        final_rankings_table = run_power_analysis(
            aggregated_df_summed.copy(),
            f"AGGREGATE RANKINGS UP TO: {end_date_dt.strftime('%Y-%m-%d')}",
            event=event,
        )

        # Add summary metrics to the event
        event.add(
            time_periods_processed=time_periods_processed,
            total_rows_fetched=total_rows_fetched,
            final_analysis_generated=final_rankings_table is not None,
        )

        # if final_rankings_table:
        #     gemini_analysis(final_rankings_table)

# endregion
