from rashdf import RasPlanHdf
import pandas as pd
from datetime import datetime
import dask.dataframe as dd
from dask import delayed, compute
from dask.distributed import Client, LocalCluster
from typing import List


@delayed
def process_wse_batch(s3_keys_batch: List[str], batch_id: int):
    """
    Processes a batch of ras plan S3 keys to extract and aggregate water surface elevation data.

    Args:
        s3_keys_batch (List[str]): A list of S3 keys pointing to the HDF files to be processed.
        batch_id (int): The batch ID for logging purposes.

    Returns:
        pd.DataFrame: A concatenated dataframe containing the processed data for the batch.
    """
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


def main(plan_files_txt_file: str, bucket_name: str, output_pq_name: str, save_all_batches: bool = False):
    """
    Main function to process water surface elevation data from HDF files listed in a text file.

    Args:
        plan_files_txt_file (str): Path to the text file containing the list of HDF file keys.
        bucket_name (str): The S3 bucket name where the HDF files are stored.
        output_pq_name (str): The name of the output parquet file to store the aggregated data.
        save_all_batches (bool, optional): If True, save each batch separately as a parquet file, useful for handling failures. Default is False.
    """
    with open(plan_files_txt_file, "r") as f:
        plan_files = f.readlines()
    s3_keys = [f"s3://{bucket_name}/{p.strip()}" for p in plan_files]

    n_batches = 100
    batches = [s3_keys[i : i + n_batches] for i in range(0, len(s3_keys), n_batches)]
    delayed_dfs = [process_wse_batch(batch, i) for i, batch in enumerate(batches)]
    dfs = compute(*delayed_dfs)

    for i, result_df in enumerate(dfs):
        if save_all_batches:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            filename = f"summary_{i}_{timestamp}.parquet"
            result_df.to_parquet(filename)
            print(f"Batch {i} complete")
        else:
            print(f"Batch {i} complete")

    dask_dfs = [dd.from_pandas(df, npartitions=1) for df in dfs]
    summary_df = dd.concat(dask_dfs, axis=1)

    summary_df.compute().to_parquet(output_pq_name, index=False)


if __name__ == "__main__":
    plan_files_txt_file = "ElkMiddle-plan_files.txt"
    bucket_name = "kanawha-pilot"
    output_pq_name = "summary.parquet"
    cluster = LocalCluster()
    client = Client(cluster)
    main(plan_files_txt_file, bucket_name, output_pq_name)
