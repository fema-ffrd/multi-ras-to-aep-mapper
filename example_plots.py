import json

import pandas as pd
from rashdf import RasPlanHdf
from utils import *

# Read in cells parquet which contains depth data for all events and their cell ids
# Only needed for block maxima example
cells = pd.read_parquet("ElkMiddleMaxDepth.parquet")
# Read in block metadata , used only for block maxima example
with open("blockfile_10_by_500.json", "r") as f:
    meta = json.load(f)

# Read in ranked block maxima data for realization and plan hdf file
ranked = pd.read_parquet("elkmiddle_r1_values.parquet")
rasplan = RasPlanHdf("Template.p01.hdf")

# Read mesh data which contains geometry and min_ws for each cell_id
mesh_foundational_data = rasplan.mesh_cell_polygons()[["geometry", "min_ws"]]
# Using the mesh data and ranked block maxima data, compute the max depth for each cell
max_depths = cell_depths(ranked, mesh_foundational_data["min_ws"].values)

realization = 1
block_index = 5
cell_id = 14205

# -----------------block maxima example-----------------#

plot_block_max(cells, meta, realization, block_index, cell_id)

# ---------------Convert mesh to gpkg--------------------#

# gdf = mesh_to_gpkg(mesh_foundational_data, max_depths)

# -----------------Realization example-----------------#

plot_max_wsel(ranked, realization, cell_id)

# -----------------AEP example-----------------#

plot_recurrence_intervals(max_depths, cell_id)

# ---------------Plot all realizations-----------------#

plot_recurrence_all_realizations(mesh_foundational_data, cell_id, pq_directory=".")
plot_all_realization_depths(mesh_foundational_data, cell_id, pq_directory=".")
