"""Main module for Pokémon Meta Analyzer."""

import os
from datetime import datetime, timedelta

# Imports
import pandas as pd

from src import config
from src.analysis import run_power_analysis
from src.data import get_deck_slugs, get_time_slice_data
from src.logging import LoggingContext, add_context, setup_logging


def main():
    """Main function to run the Pokémon Meta Analyzer."""
    setup_logging(debug_mode=False)

    with LoggingContext("pokemon_matchup_analysis") as logger:
        logger.info("Job Started")
        os.makedirs(config.DATA_FOLDER, exist_ok=True)

        start_date_dt = config.START_DATE
        end_date_dt = datetime.now()

        add_context(
            date_range_start=start_date_dt.strftime("%Y-%m-%d"),
            date_range_end=end_date_dt.strftime("%Y-%m-%d"),
        )

        # Variables for loop
        time_slice_start_dt = start_date_dt
        all_dfs = []
        time_periods_processed = 0
        total_rows_fetched = 0

        # --- THE MAIN LOOP ---
        while time_slice_start_dt <= end_date_dt:
            time_slice_end_dt = time_slice_start_dt + timedelta(
                days=config.TIME_SLICE_DAYS
            )

            # Format dates
            ts_start_str = time_slice_start_dt.strftime("%Y-%m-%d")
            ts_end_str = time_slice_end_dt.strftime("%Y-%m-%d")

            logger.info(
                f"{'=' * 25} PROCESSING TIME: {ts_start_str} to {ts_end_str} {'=' * 25}"
            )

            # 1. Fetch Decks
            deck_list = get_deck_slugs(
                start_date=ts_start_str, end_date=ts_end_str
            )

            # 2. Fetch Data
            is_ongoing = time_slice_start_dt <= end_date_dt <= time_slice_end_dt
            raw_df = get_time_slice_data(
                ts_start_str,
                ts_end_str,
                is_ongoing,
                deck_list,
                config.DATA_FOLDER,
            )

            # 3. Analyze Slice
            if raw_df is not None and not raw_df.empty:
                all_dfs.append(raw_df)
                total_rows_fetched += len(raw_df)
                time_periods_processed += 1

                run_power_analysis(
                    raw_df.copy(),
                    f"RANKINGS FOR TIME SLICE: {ts_start_str} to {ts_end_str}",
                )

            # Increment Loop
            time_slice_start_dt += timedelta(days=7)

        # --- FINAL AGGREGATION ---
        if all_dfs:
            aggregated_df = pd.concat(all_dfs, ignore_index=True)
            # Sum up wins/totals for the aggregate
            aggregated_df_summed = (
                aggregated_df.groupby(["deck1", "deck2"]).sum().reset_index()
            )

            final_table = run_power_analysis(
                aggregated_df_summed.copy(),
                f"AGGREGATE RANKINGS UP TO: {end_date_dt.strftime('%Y-%m-%d')}",
            )

            # --- AI ANALYSIS ---
            if final_table:
                add_context(final_analysis_generated=True)
                # gemini_analysis(final_table)

        add_context(
            time_periods_processed=time_periods_processed,
            total_rows_fetched=total_rows_fetched,
        )


if __name__ == "__main__":
    main()
