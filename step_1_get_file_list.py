"""
This script searches for S3 objects with a given prefix in a specified bucket. For monte carlo analyses, there 
may be many millions of files. This script should be run one time and the output used for downstream processing.
"""

import boto3


def search_s3_objects(
    bucket_name,
    prefix="FFRD_Kanawha_Compute/sims/uncertainty_10_by_500_no_bootstrap_5_10a_2024",
    suffix="ras/ElkMiddle/ElkMiddle.p01.hdf",
):
    """
    Search for S3 objects with a given prefix in a specified bucket.
    Handles pagination to deal with more than 1000 objects.
    Only returns objects with the specified suffix.

    :param bucket_name: Name of the S3 bucket
    :param prefix: Prefix of the objects to search for
    :return: List of object keys matching the prefix with .hdf extension
    """
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    object_keys = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith(suffix) and prefix in obj["Key"]:
                    object_keys.append(obj["Key"])

    return object_keys


if __name__ == "__main__":
    bucket_name = "kanawha-pilot"
    ras_model = "ElkMiddle"
    output_file = f"{ras_model}-plan_files.txt"

    object_keys = search_s3_objects(bucket_name)

    with open(output_file, "w") as f:
        for key in object_keys:
            f.write(key + "\n")
