import argparse
import sys
from datetime import datetime, timezone

import pandas as pd

from src import extract, load, transform, validation


def run(batch_id):
    spotify = extract.extract_spotify(batch_id)
    validation.validate_spotify_raw(pd.read_parquet(spotify["path"]), source=spotify["source"])

    grammy = extract.extract_grammys(batch_id)
    validation.validate_grammys_raw(pd.read_parquet(grammy["path"]), source=grammy["source"])

    prepared = transform.transform_and_integrate(spotify["path"], grammy["path"], batch_id)
    result = validation.validate_prepared(transform.read_prepared(prepared["prepared_dir"]), source=prepared["prepared_dir"])
    return load.load_dw(prepared["prepared_dir"], result, batch_id)


def main():
    parser = argparse.ArgumentParser(description="Run the whole ETL pipeline once")
    parser.add_argument("--batch-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    args = parser.parse_args()
    try:
        run(args.batch_id)
    except validation.DataQualityError as error:
        print(f"PIPELINE STOPPED: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
