import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from google import genai

START_DATE = datetime.strptime("2025-11-08", "%Y-%m-%d")
PLAYER_COUNT_MINIMUM = 50
# Top Percentage Place e.g. 0.25 -> 25%
PLACING_PERCENTAGE = 0.25
PLATFORM = "all"  # inperson
DECKS = [
    "gholdengo-lunatone",
    "dragapult-dusknoir",
    "charizard-pidgeot",
    "gardevoir-ex-sv",
    "mega-absol-box",
    "gardevoir-jellicent",
    "ceruledge-ex",
    "grimmsnarl-froslass",
    "n-zoroark",
    "raging-bolt-ogerpon",
    "tera-box",
    "flareon-noctowl",
    "alakazam-dudunsparce",
    "joltik-box",
]
GEMINI_API_KEY = "AIzaSyBggEeoQFMVZj1NwJLnEVwwcNP1DfYylPI"

# ==============================================================================
# PART 1: DATA FETCHING AND CACHING
# ==============================================================================


def fetch_matchup_data(start_date, end_date):
    """
    Fetches matchup data from TrainerHill for a given date range.
    Returns a raw pandas DataFrame.
    """
    logging.debug(f"Calling API to fetch data from {start_date} to {end_date}...")

    url = "https://www.trainerhill.com/_dash-update-component"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "referer": "https://www.trainerhill.com/meta?game=PTCG",
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
                    "game": "PTCG",
                    "division": ["SR", "MA"],
                },
            },
            {"id": "meta-archetype-select", "property": "value", "value": DECKS},
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
            logging.debug("API call successful!")
            return pd.DataFrame(matchup_list)
        else:
            logging.warning("No data returned from API for this period.")
            return pd.DataFrame()  # Return empty DataFrame

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred during API request: {e}")
        return None
    except KeyError:
        logging.error("Could not find expected data in API response.")
        return None


def post_deck_select_table(
    players: int = 50,
    start_date: str = "2025-11-29",
    end_date: str = "2025-12-20",
    platform: str = "all",
    game: str = "PTCG",
    division=None,
    session: requests.Session | None = None,
    timeout: int = 10,
):
    """Perform the same POST request shown in the curl example and return parsed JSON.

    Parameters:
        players: Minimum players value (default 50)
        start_date: YYYY-MM-DD start date
        end_date: YYYY-MM-DD end date
        platform: platform filter (default "all")
        game: game filter (default "PTCG")
        division: list of divisions, defaults to ["JR","SR","MA"]
        session: optional requests.Session to reuse cookies/headers
        timeout: request timeout in seconds

    Returns:
        The response parsed as JSON on success, or None on error.
    """
    if division is None:
        division = ["JR", "SR", "MA"]

    url = "https://www.trainerhill.com/_dash-update-component"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://www.trainerhill.com/decklist?game=PTCG",
        "Content-Type": "application/json",
        # The curl contained X-CSRFToken: undefined; preserve it by default so behaviour matches the curl
        "X-CSRFToken": "undefined",
        "Origin": "https://www.trainerhill.com",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "Priority": "u=4",
        "TE": "trailers",
    }

    payload = {
        "output": "deck-select-table.children",
        "outputs": {"id": "deck-select-table", "property": "children"},
        "inputs": [
            {
                "id": "deck-select-tour-store",
                "property": "data",
                "value": {
                    "players": players,
                    "start_date": start_date,
                    "end_date": end_date,
                    "platform": platform,
                    "game": game,
                    "division": division,
                },
            },
            {"id": "deck-select-search", "property": "value"},
        ],
        "changedPropIds": [],
        "parsedChangedPropsIds": [],
    }

    sess = session or requests
    try:
        resp = sess.post(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        # Return parsed JSON (may be nested like the curl response)
        return resp.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error performing deck-select POST: {e}")
        return None


def get_weekly_data(week_start, week_end, is_current_week, folder="pokemon_data"):
    """
    Orchestrates fetching data for a week.
    If it's the current week, it re-fetches data. Otherwise, it uses the cache.
    """
    filename = f"data_{week_start}_to_{week_end}.csv"
    filepath = os.path.join(folder, filename)

    if is_current_week and os.path.exists(filepath):
        logging.debug(
            f"Current week detected. Removing stale file to re-fetch: {filename}"
        )
        os.remove(filepath)

    if os.path.exists(filepath):
        logging.debug(f"Found local file for past week: {filename}. Loading from disk.")
        return pd.read_csv(filepath)
    else:
        raw_df = fetch_matchup_data(week_start, week_end)
        if raw_df is not None and not raw_df.empty:
            raw_df.to_csv(filepath, index=False)
            logging.debug(f"Saved data to {filepath}")
        return raw_df


# ==============================================================================
# PART 2: POWER RANKING ANALYSIS
# ==============================================================================


def run_power_analysis(df, title="FINAL POWER RANKINGS"):
    """
    Executes the iterative power ranking algorithm.
    Logs the result and returns the ranking table as a string.
    """
    if df.empty:
        logging.warning(f"Skipping analysis for '{title}' due to no data.")
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

    # Return the table string for external use (like the AI prompt)
    return table_string


# ==============================================================================
# PART 3: GEMINI AI ANALYSIS
# ==============================================================================


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


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    DATA_FOLDER = "pokemon_data"
    os.makedirs(DATA_FOLDER, exist_ok=True)

    start_date = START_DATE
    end_date = datetime.now()

    current_date = start_date
    all_weekly_dfs = []

    today = datetime.now()
    aggregated_df_summed = pd.DataFrame()

    while current_date <= end_date:
        week_start_dt = current_date
        week_end_dt = current_date + timedelta(days=6)

        week_start_str = week_start_dt.strftime("%Y-%m-%d")
        week_end_str = week_end_dt.strftime("%Y-%m-%d")

        logging.debug(
            f"{'=' * 25} PROCESSING WEEK: {week_start_str} to {week_end_str} {'=' * 25}"
        )

        is_ongoing_week = week_start_dt <= today <= week_end_dt

        weekly_raw_df = get_weekly_data(
            week_start_str, week_end_str, is_ongoing_week, DATA_FOLDER
        )

        if weekly_raw_df is not None and not weekly_raw_df.empty:
            all_weekly_dfs.append(weekly_raw_df)
            run_power_analysis(
                weekly_raw_df.copy(),
                f"RANKINGS FOR WEEK: {week_start_str} to {week_end_str}",
            )

            aggregated_df = pd.concat(all_weekly_dfs, ignore_index=True)
            aggregated_df_summed = (
                aggregated_df.groupby(["deck1", "deck2"]).sum().reset_index()
            )

        current_date += timedelta(days=7)

    # Final aggregate analysis for the entire period
    final_rankings_table = run_power_analysis(
        aggregated_df_summed.copy(),
        f"AGGREGATE RANKINGS UP TO: {end_date.strftime('%Y-%m-%d')}",
    )

    # if final_rankings_table:
    #     gemini_analysis(final_rankings_table)
