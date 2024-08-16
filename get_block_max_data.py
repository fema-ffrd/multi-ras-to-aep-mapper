import json
from typing import List, Dict, Tuple
import pandas as pd
import dask.dataframe as dd


def create_block_event(meta: List[Dict], realization: int) -> Dict[int, List[int]]:
    """Function to collect the event ids for each block in the realization. Takes in the block metadata and the realization
    and returns a dictionary with the block id as the keys and the event ids as the values.
    """

    block_event = {}
    for block in meta:
        if block["realization_index"] == realization:
            block_index = block["block_index"]
            block_event_start = block["block_event_start"]
            block_event_end = block["block_event_end"]
            event_nums = list(range(block_event_start, block_event_end + 1))

            block_event[block_index] = event_nums
    return block_event


def highest_values_and_events(all_events_df: pd.DataFrame, columns: List[str]) -> Tuple[dd.Series, dd.Series]:
    """
    Calculate the highest values and corresponding events from the given dataframe for a given list of columns.
    Returns a tuple of series containing the highest values and associated event_ids (column name) for each cell_id(row).
    """

    existing_columns = [col for col in columns if col in all_events_df.columns]
    if not existing_columns:
        raise KeyError("No valid columns found for processing.")
    max_value = all_events_df[existing_columns].max(axis=1)
    max_event = all_events_df[existing_columns].idxmax(axis=1)
    return max_value, max_event


def get_block_maxima(
    all_events_df: pd.DataFrame, sorted_block_event: Dict[int, List[int]], num_blocks: int = 500
) -> Tuple[dd.DataFrame, dd.DataFrame]:
    """
    Process each block and collect the results into Dask DataFrames.
    Returns two dask dataframes, one for highest values and one for highest events for each block.
    """
    highest_values_results = []
    highest_events_results = []
    for block_id, events in sorted_block_event.items():
        if block_id in range(0, num_blocks + 1):
            print(f"Processing block_id: {block_id}")
            events = [str(event) for event in events]
            max_values, max_events = highest_values_and_events(all_events_df, events)
            highest_values_results.append(max_values.rename(block_id))
            highest_events_results.append(max_events.rename(block_id))
    highest_vals_ddf = dd.concat(highest_values_results, axis=1)
    highest_events_ddf = dd.concat(highest_events_results, axis=1)
    return highest_vals_ddf, highest_events_ddf


def rank_block_maxima(values_df: pd.DataFrame, events_df: pd.DataFrame, events: bool = False) -> pd.DataFrame:
    """
    Sort the given values and events dataframes, making sure that the event index is aligned with the associated sorted value index.
    If events is True, return the sorted events df, otherwise return the sorted values df.
    """
    num_columns = len(values_df.columns)
    ranked_values_df = pd.DataFrame(index=values_df.index, columns=range(1, num_columns + 1))
    ranked_events_df = pd.DataFrame(index=events_df.index, columns=range(1, num_columns + 1))
    for idx in values_df.index:
        combined = pd.DataFrame({"value": values_df.loc[idx], "event": events_df.loc[idx]})
        combined_sorted = combined.sort_values(by="value")
        ranked_values_df.loc[idx] = combined_sorted["value"].values
        ranked_events_df.loc[idx] = combined_sorted["event"].values

    if events:
        return ranked_events_df
    else:
        return ranked_values_df


def main(all_events_pq: str, blockfile: str, realization: int, output_prefix: str) -> None:
    """
    Main function to process the cells and block metadata, compute the highest values and events,
    rank them, and save the results to parquet files.
    """

    all_events_df = pd.read_parquet(all_events_pq)

    with open(blockfile, "r") as f:
        meta = json.load(f)

    sorted_block_event_dict = create_block_event(meta, realization)
    highest_values_ddf, highest_events_ddf = get_block_maxima(all_events_df, sorted_block_event_dict, num_blocks=500)

    ranked_events_ddf = dd.map_partitions(
        rank_block_maxima,
        highest_values_ddf,
        highest_events_ddf,
        events=True,
    )
    ranked_values_ddf = dd.map_partitions(
        rank_block_maxima,
        highest_values_ddf,
        highest_events_ddf,
        events=False,
    )
    ranked_events_ddf.compute().to_parquet(f"{output_prefix}_r{realization}_events.parquet")
    ranked_values_ddf.compute().to_parquet(f"{output_prefix}_r{realization}_values.parquet")


if __name__ == "__main__":
    all_events_pq = "ElkMiddleMaxDepth.parquet"
    blockfile = "blockfile_10_by_500.json"
    realization = 5
    output_prefix = "elkmiddle"

    main(all_events_pq, blockfile, realization, output_prefix)
