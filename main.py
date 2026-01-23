"""Main orchestration module for Pokémon Meta Analyzer."""

import os
from datetime import datetime

from src import config
from src.analysis import aggregate_matchup_data, run_power_analysis
from src.data import generate_weekly_slices, get_deck_slugs, get_time_slice_data
from src.logger_utils import LoggingContext, add_context, setup_logging


def main():
    """Run the Pokémon Meta Analyzer job."""
    setup_logging(debug_mode=False)

    with LoggingContext("pokemon_matchup_analysis") as logger:
        logger.info("Job Started")

        # Setup
        os.makedirs(config.DATA_FOLDER, exist_ok=True)
        start_dt = config.START_DATE
        end_dt = datetime.now()

        add_context(
            date_range_start=start_dt.strftime("%Y-%m-%d"),
            date_range_end=end_dt.strftime("%Y-%m-%d"),
        )

        all_dfs = []
        slices_processed = 0
        total_rows = 0

        # --- PROCESS TIME SLICES ---
        for start_str, end_str, is_ongoing in generate_weekly_slices(
            start_dt, end_dt
        ):
            logger.info(
                f"{'=' * 25} PROCESSING: {start_str} to {end_str} {'=' * 25}"
            )

            # 1. Fetch
            deck_list = get_deck_slugs(start_date=start_str, end_date=end_str)

            # 2. Get Data
            df = get_time_slice_data(
                start_str, end_str, is_ongoing, deck_list, config.DATA_FOLDER
            )

            # 3. Analyze
            if df is not None and not df.empty:
                all_dfs.append(df)
                total_rows += len(df)
                slices_processed += 1

                run_power_analysis(
                    df.copy(), f"RANKINGS: {start_str} to {end_str}"
                )

        # --- FINAL AGGREGATION ---
        if all_dfs:
            final_df = aggregate_matchup_data(all_dfs)

            final_table = run_power_analysis(
                final_df,
                f"AGGREGATE RANKINGS UP TO: {end_dt.strftime('%Y-%m-%d')}",
            )

            if final_table:
                add_context(final_analysis_generated=True)
                # gemini_analysis(final_table)

        # Final Telemetry
        add_context(
            time_periods_processed=slices_processed,
            total_rows_fetched=total_rows,
        )


if __name__ == "__main__":
    main()
