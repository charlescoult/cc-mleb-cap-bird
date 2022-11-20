import dataflow
from schema import apply_schema

import os, sys
import datetime
import requests
import json
import pandas as pd
from tqdm import tqdm
import numpy as np

stage_prefix = '02'

source_data_fn = dataflow.get_prev_fn( stage_prefix )

def save_parquet( df, filename ):
    print(f'Saving parquet: {filename}, {df.shape}')
    df.to_parquet(filename)

def main():

    # open parquet file if it exists
    df = pd.read_parquet( source_data_fn )

    # apply schema to df
    df = apply_schema( df )

    # save to new file with stage prefix
    save_parquet( df, dataflow.get_fn( stage_prefix ) )

def exit_on_error():
    try:
        sys.exit(1)
    except SystemExit:
        os._exit(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        exit_on_error()
    except FileNotFoundError:
        print(f"File { source_data_fn } not found.")
        exit_on_error()

