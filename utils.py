from matplotlib import pyplot as plt
import geopandas as gpd
import pandas as pd
import os
from typing import List, Dict


def get_realization_data(realization: int, cells: pd.DataFrame, metadata: List[Dict]) -> pd.DataFrame:
    for m in metadata:
        if m["realization_index"] == realization:
            if m["block_index"] == 1:
                start = m["block_event_start"]
            elif m["block_index"] == 500:
                stop = m["block_event_end"]
                break

    sims = [str(i) for i in range(start, stop + 1) if str(i) in cells.columns]

    realization_cells = cells[sims]
    return realization_cells


def cell_depths(maximas: pd.DataFrame, minimum: pd.Series) -> pd.DataFrame:
    """
    Computes the depth difference between maximas and minimum water surface elevations.
    """

    depths = {}
    for i in maximas.index:
        try:
            cell_data = maximas.iloc[i].values
            depth_data = cell_data - minimum[i]
            depths[i] = depth_data
        except:
            print("Failed")
    return pd.DataFrame.from_dict(depths, orient="index")


def plot_all_realization_depths(mesh_foundational_data: gpd.GeoDataFrame, cell_id: int, pq_directory: str) -> None:
    """
    Plots maximum depths for all realizations at a given cell ID using the min_ws from the mesh_foundational_data gdf
    and the ranked block maxima data for each realization.
    """

    _, ax = plt.subplots()
    for i in range(1, 6):
        file_path = os.path.join(pq_directory, f"elkmiddle_r{i}_values.parquet")
        ranked = pd.read_parquet(file_path)
        max_depths = cell_depths(ranked, mesh_foundational_data["min_ws"].values)
        x, y = max_depths.iloc[cell_id].index, max_depths.iloc[cell_id].values
        ax.set_xlabel("Simulation Rank")
        ax.set_ylabel("Depth (ft)")
        ax.set_title(f"Maximum Depth all realizations at \nCell {cell_id}")
        ax.scatter(x, y, label=f"Realization {i}", alpha=0.8, s=8)
        ax.legend()
    plt.show()


def plot_recurrence_all_realizations(mesh_foundational_data: gpd.GeoDataFrame, cell_id: int, pq_directory: str) -> None:
    """
    Plots the recurrence intervals for all realizations at a given cell ID using the min_ws from the mesh_foundational_data gdf
    and the ranked block maxima data for each realization.
    """
    _, ax = plt.subplots()
    for i in range(1, 6):
        file_path = os.path.join(pq_directory, f"elkmiddle_r{i}_values.parquet")
        ranked = pd.read_parquet(file_path)
        max_depths = cell_depths(ranked, mesh_foundational_data["min_ws"].values)

        numerator = [1, 5, 10, 20, 50, 100, 250]
        max_sim = 500
        rank_ids = [max_sim - i for i in numerator]
        tick_positions = [round(1 - i / max_sim, 3) for i in rank_ids]

        _, y = max_depths.iloc[cell_id].index, max_depths.iloc[cell_id].values
        plt.xscale("log")
        ax.set_xlabel("Recurrence Interval (years)")
        ax.set_ylabel("Depth (ft)")
        ax.set_title(f"Recurrence Interval all Realizations at \nCell {cell_id}")

        ax.set_xticks(tick_positions)

        labels = [int(1 / i) for i in tick_positions]
        ax.set_xticklabels(sorted(labels, reverse=False))

        ax.scatter(tick_positions, sorted(y[rank_ids]), label=f"Realization {i}", alpha=0.8, s=8)
        ax.legend()
    plt.show()


def plot_block_max(cells: pd.DataFrame, meta: List[Dict], realization: int, block_index: int, cell_id: int) -> None:
    """
    Plots the depths of each event id for a specific block in a given realization at a given cell. Reads from the total cells dataframe and
    the meta list which contains block data.
    """
    realization_cells = get_realization_data(realization, cells, meta)
    for item in meta:
        if item["realization_index"] == realization and item["block_index"] == block_index:
            block = realization_cells[[str(i) for i in range(item["block_event_start"], item["block_event_end"] + 1)]]

    fig, ax = plt.subplots()
    x, y = block.iloc[cell_id].index, block.iloc[cell_id]
    ax.set_xlabel("Event ID")
    ax.set_ylabel("WSEL (ft)")
    ax.set_title(f"WSEL for each event from Realization {realization}, Block {block_index} \nat Cell {cell_id}")
    ax.scatter(x, y, color="black")  # change color if necessary
    plt.show()


def mesh_to_gpkg(
    mesh_foundational_data: gpd.GeoDataFrame,
    max_depths: pd.DataFrame,
    cols_range: int = 500,
    output_file: str = "mesh.gpkg",
) -> gpd.GeoDataFrame:
    """
    Converts mesh data and maximum depths to a GeoPackage file using the mesh geom from the mesh_foundational_data gdf and
    max depths from the max_depths df.
    """
    cols = [i for i in range(0, cols_range)]

    mesh_block_summary = pd.concat([mesh_foundational_data, max_depths], axis=1)

    rename_dict = {i: f"rank_{i}" for i in cols}

    mesh_block_summary.rename(columns=rename_dict, inplace=True)

    gdf = gpd.GeoDataFrame(
        data=mesh_block_summary[[f"rank_{i}" for i in cols]], geometry=mesh_foundational_data["geometry"]
    )
    gdf.to_file(output_file, driver="GPKG")

    return gdf


def plot_max_wsel(max_depths: pd.DataFrame, realization: int, cell_id: int) -> None:
    """
    Plots the maximum depths for a given cell ID from the max_depths DataFrame.
    """
    fig, ax = plt.subplots()
    x, y = max_depths.iloc[cell_id].index, max_depths.iloc[cell_id].values
    ax.set_xlabel("Simulation ID")
    ax.set_ylabel("WSEL (ft)")
    ax.set_title(f"Sorted Maximum WSEL from Realization {realization} at \nCell {cell_id}")
    ax.scatter(x, y, color="black")  # change color if necessary
    plt.show()


def plot_recurrence_intervals(max_depths: pd.DataFrame, cell_id: int, max_sim: int = 500) -> None:
    """
    Plots the recurrence intervals for a given cell ID from the max_depths df.
    """
    numerator = [1, 5, 10, 20, 50, 100, 250]
    max_sim = 500
    rank_ids = [max_sim - i for i in numerator]
    tick_positions = [round(1 - i / max_sim, 3) for i in rank_ids]

    fig, ax = plt.subplots()
    x, y = max_depths.iloc[cell_id].index, max_depths.iloc[cell_id].values
    plt.xscale("log")
    ax.set_xlabel("Recurrence Interval (years)")
    ax.set_ylabel("Depth (ft)")
    ax.set_title(f"Recurrence Interval all Realizations at \nCell {cell_id}")

    ax.set_xticks(tick_positions)

    labels = [int(1 / i) for i in tick_positions]
    ax.set_xticklabels(sorted(labels, reverse=False))

    ax.scatter(tick_positions, sorted(y[rank_ids]), color="black")
    plt.show()
