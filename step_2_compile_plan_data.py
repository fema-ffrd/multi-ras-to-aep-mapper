from datetime import datetime
from rashdf import RasPlanHdf
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
from dask.distributed import Client, LocalCluster


@delayed
def process_wse_batch(s3_keys_batch, batch_id):
    dfs = []
    for i, s3_key in enumerate(s3_keys_batch):
        if i % 10 == 0:
            print(f"Processing batch {batch_id}, item {i}")

        sim_id = s3_key.split("uncertainty_10_by_500_no_bootstrap_5_10a_2024")[1].split("ras")[0].strip("/")
        df = RasPlanHdf.open_uri(s3_key)
        data = df.mesh_max_ws()
        temp_df = pd.DataFrame(data["max_ws"].values, columns=["max_ws"])

        if "cell_id" in data:
            temp_df["cell_id"] = data["cell_id"]
            temp_df.set_index("cell_id", inplace=True, drop=True)
            temp_df.rename(columns={"max_ws": sim_id}, inplace=True)

        dfs.append(temp_df)

    local_df = pd.concat(dfs, axis=1)
    return local_df


def main():
    with open("/home/ubuntu/workbench/repos/ElkMiddle-plan_files.txt", "r") as f:
        plan_files = f.readlines()
    s3_keys = [f"s3://kanawha-pilot/{p.strip()}" for p in plan_files]

    n_batches = 100
    batches = [s3_keys[i : i + n_batches] for i in range(0, len(s3_keys), n_batches)]

    delayed_dfs = [process_wse_batch(batch, i) for i, batch in enumerate(batches)]
    dfs = compute(*delayed_dfs)

    for i, result_df in enumerate(dfs):
        # Uncomment to store each batch as a separate parquet file (useful for handling failures)
        # timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        # filename = f"summary_{i}_{timestamp}.parquet"
        # result_df.to_parquet(filename)
        print(f"Batch {i} complete")

    summary_df = dd.concat([dd.from_pandas(df, npartitions=1) for df in dfs], axis=1)
    summary_df.to_parquet("summary-dask")


if __name__ == "__main__":
    cluster = LocalCluster()
    client = Client(cluster)
    main()
