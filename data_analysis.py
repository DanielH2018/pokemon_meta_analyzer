"""Run Data Analysis on Pokemon TCG Matchup Data."""

import contextvars
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import structlog
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

# --- 1. Context Variable for Logger ---

_logger_ctx = contextvars.ContextVar("logger_ctx")


# --- 2. The Enhanced Context Manager ---
class LoggingContext:
    """A context manager for logging script execution details."""

    def __init__(self, script_name: str, **initial_vars):
        """Initialize the LoggingContext."""
        self.script_name = script_name
        self.initial_vars = initial_vars
        self.start_time = 0.0
        self.token = None

    def __enter__(self):
        """Enter the context manager."""
        self.start_time = time.time()

        # Initialize logger with script name and any starting vars
        logger = structlog.get_logger().bind(
            script=self.script_name, **self.initial_vars
        )

        # Set the context variable
        self.token = _logger_ctx.set(logger)
        return logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and log the summary event."""
        # 1. Calculate Duration
        duration = (time.time() - self.start_time) * 1000

        # 2. Get the current logger (which includes everything added via add_context!)
        logger = get_logger()

        # 3. Handle Status & Errors
        if exc_type:
            status = "failed"
            # Bind error details to the logger for the final message
            logger = logger.bind(
                error=str(exc_val), error_type=exc_type.__name__
            )
        else:
            status = "success"

        # 4. The "Wide Event" Log
        logger.info(
            "flight_recorder_summary",
            status=status,
            duration_ms=round(duration, 2),
        )

        # Cleanup context
        if self.token:
            _logger_ctx.reset(self.token)

        # Return False to let the exception propagate (crash the script)
        return False


# --- 3. Helper Functions ---


def setup_logging(debug_mode: bool = False):
    """Setup structlog logging configuration."""
    # 1. Decide: Pretty or JSON?
    if sys.stdout.isatty():
        # If running in a terminal, use colors and columns
        renderer = structlog.dev.ConsoleRenderer()
    else:
        # If running in Docker or piped to a file, use JSON
        renderer = structlog.processors.JSONRenderer()

    # 2. Decide: Debug or Info?
    min_level = logging.DEBUG if debug_mode else logging.INFO

    structlog.configure(
        processors=[
            # Add a timestamp
            structlog.processors.TimeStamper(fmt="iso"),
            # Add log level (INFO, ERROR)
            structlog.processors.add_log_level,
            # Perform the rendering we chose above
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(min_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger() -> structlog.BoundLogger:
    """Get the current logger."""
    try:
        return _logger_ctx.get()
    except LookupError:
        return structlog.get_logger()


def add_context(**kwargs):
    """Add context variables to the current logger."""
    try:
        # Get current logger
        current_logger = _logger_ctx.get()
        # Bind new values
        new_logger = current_logger.bind(**kwargs)
        # Update the context variable with the new logger
        _logger_ctx.set(new_logger)
    except LookupError:
        pass  # Or raise warning if used outside context


# endregion


# ==============================================================================
# PART 1: DATA FETCHING AND CACHING
# ==============================================================================

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
            {
                "id": "meta-placing",
                "property": "value",
                "value": PLACING_PERCENTAGE,
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
    players: int = PLAYER_COUNT_MINIMUM,
    start_date: str = START_DATE.strftime("%Y-%m-%d"),
    end_date: str = datetime.now().strftime("%Y-%m-%d"),
    platform: str = PLATFORM,
    game: str = GAME,
    division: list[str] = DIVISION,
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

# ==============================================================================
# PART 2: POWER RANKING ANALYSIS
# ==============================================================================

# region Power Ranking Analysis


def run_power_analysis(df, title="FINAL POWER RANKINGS"):
    """Executes the iterative power ranking algorithm.

    Logs the result and returns the ranking table as a string.

    Args:
        df: A pandas DataFrame containing the matchup data.
        title: Title of the analysis (default is "FINAL POWER RANKINGS").

    Returns:
        A string representation of the power rankings table.
    """
    logger = get_logger()
    if df.empty:
        logger.warning(f"Skipping analysis for '{title}' due to no data.")
        add_context(power_analysis_empty_data=True)
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
        logger.debug(f"Iteration {i + 1} complete for '{title}'.")

    # --- Final Output ---
    summary_df = pd.DataFrame(index=all_decks)
    summary_df["power_rank_score"] = (power_scores * 1000).round(2)
    summary_df = summary_df.sort_values(by="power_rank_score", ascending=False)

    table_string = summary_df.head(15).to_string()

    logger.info(f"\n{'=' * 50}\n {title}\n{'=' * 50}\n{table_string}")

    add_context(
        power_analysis_decks_analyzed=len(all_decks),
        power_analysis_iterations=20,
    )

    # Return the table string for external use (like the AI prompt)
    return table_string


# endregion

# ==============================================================================
# PART 3: GEMINI AI ANALYSIS
# ==============================================================================

# region AI Analysis


def gemini_analysis(final_rankings_table):
    """Analyzes Pokémon meta-game rankings using Gemini AI.

    Sends the power rankings table to the Gemini API for analysis and logs
    insights about the current meta-game state, including top decks, sleepers,
    and overall meta health.

    Args:
        final_rankings_table: A string containing the formatted power rankings
            table to be analyzed.
    """
    logger = get_logger()
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
        """  # noqa: E501

        logger.info("\n\n{'='*20} 🤖 GEMINI META ANALYSIS 🤖 {'='*20}")

        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
        )

        logger.info(response.text)
    except Exception as e:
        logger.error(f"\n\nAn error occurred during Gemini analysis: {e}")


# endregion

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

# region Main

if __name__ == "__main__":
    setup_logging(debug_mode=False)
    with LoggingContext("pokemon_matchup_analysis") as logger:
        logger.info("Job Started")
        os.makedirs(DATA_FOLDER, exist_ok=True)

        # Create Date Time Range
        start_date_dt = START_DATE
        end_date_dt = datetime.now()

        # Add date range to the context for tracking
        add_context(
            date_range_start=start_date_dt.strftime("%Y-%m-%d"),
            date_range_end=end_date_dt.strftime("%Y-%m-%d"),
        )

        # Initialize variables for time slicing and data aggregation
        time_slice_start_dt = start_date_dt
        all_dfs = []
        aggregated_df_summed = pd.DataFrame()
        time_periods_processed = 0
        total_rows_fetched = 0

        while time_slice_start_dt <= end_date_dt:
            # Define the time slice for this iteration
            time_slice_end_dt = time_slice_start_dt + timedelta(days=6)

            # Format to YYYY-MM-DD format for API calls and logging
            time_slice_start_str = time_slice_start_dt.strftime("%Y-%m-%d")
            time_slice_end_str = time_slice_end_dt.strftime("%Y-%m-%d")

            logger.info(
                f"{'=' * 25} PROCESSING TIME: {time_slice_start_str} to "
                f"{time_slice_end_str} {'=' * 25}"
            )

            # Check if the current time slice includes the end date
            is_ongoing_time = (
                time_slice_start_dt <= end_date_dt <= time_slice_end_dt
            )

            # Fetch deck slugs for the current time slice
            deck_list = get_deck_slugs(
                start_date=time_slice_start_str,
                end_date=time_slice_end_str,
            )

            # Fetch data for the current time slice
            raw_df = get_time_slice_data(
                time_slice_start_str,
                time_slice_end_str,
                is_ongoing_time,
                deck_list,
                DATA_FOLDER,
            )

            # Process the fetched data if it's valid
            if raw_df is not None and not raw_df.empty:
                all_dfs.append(raw_df)
                total_rows_fetched += len(raw_df)
                time_periods_processed += 1
                time_slice_label = (
                    f"RANKINGS FOR TIME SLICE: {time_slice_start_str} to "
                    f"{time_slice_end_str}"
                )
                run_power_analysis(
                    raw_df.copy(),
                    time_slice_label,
                )

                aggregated_df = pd.concat(all_dfs, ignore_index=True)
                aggregated_df_summed = (
                    aggregated_df.groupby(["deck1", "deck2"])
                    .sum()
                    .reset_index()
                )

            time_slice_start_dt += timedelta(days=7)

        # Final aggregate analysis for the entire period
        final_rankings_table = run_power_analysis(
            aggregated_df_summed.copy(),
            f"AGGREGATE RANKINGS UP TO: {end_date_dt.strftime('%Y-%m-%d')}",
        )

        # Add summary metrics to the context
        add_context(
            time_periods_processed=time_periods_processed,
            total_rows_fetched=total_rows_fetched,
            final_analysis_generated=final_rankings_table is not None,
        )

        # Execute AI analysis on the final rankings table
        # if final_rankings_table:
        #     gemini_analysis(final_rankings_table)

# endregion
