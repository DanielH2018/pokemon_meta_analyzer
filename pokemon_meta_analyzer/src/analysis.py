"""Analysis functions for Pokémon meta-game data."""

import pandas as pd
from google import genai

from src import config
from src.logging import add_context, get_logger


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

    logger.info(f"\n{'=' * 50}\n {title}\n{'=' * 50}\n{table_string}\n")

    add_context(
        power_analysis_decks_analyzed=len(all_decks),
        power_analysis_iterations=20,
    )

    # Return the table string for external use (like the AI prompt)
    return table_string


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
        client = genai.Client(api_key=config.GEMINI_API_KEY)

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
