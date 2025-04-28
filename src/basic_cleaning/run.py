#!/usr/bin/env python
"""
Download the raw dataset from W&B and apply basic data cleaning steps, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    logger.info("Perfoming data cleaning..")
    run = wandb.init(group="basic_cleaning")

    # retrieve the dataset artifact 
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info(f"Artifact successfully downloaded to {artifact_local_path}.\nLoading the data...")
    df = pd.read_csv(artifact_local_path)

    # Perform data cleansing
    logger.info("Performing data cleansing..")
    valid_prices = df["price"].between(args.min_price, args.max_price)
    df_valid = df[valid_prices].copy() # remove outliers
    df_valid["last_review"] = pd.to_datetime(df_valid["last_review"])
    idx = df_valid["longitude"].between(-74.25, -73.50) & df_valid["latitude"].between(40.5, 41.2) # new step to fix error with sample2.csv
    df_valid = df_valid[idx].copy()

    # Persist cleaned data
    file_out = "clean_sample.csv"
    df_valid.to_csv(file_out, index=False)
    logger.info(f"Processed artifact saved locally ({file_out})")
    artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type, 
            description=args.output_description
            )
    artifact.add_file(file_out)
    run.log_artifact(artifact)
    run.finish()
    logger.info("Processed artifact saved remotely (W&B)")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="name of input artifact (raw database)",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="name of output artifact (clean database)",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="the type of the output file",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="a text describing the output file",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum valid price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="the maximum valid price",
        required=True
    )


    args = parser.parse_args()

    go(args)
