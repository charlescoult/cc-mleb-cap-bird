import dataflow

import os, sys
import datetime
import requests
import json
import pandas as pd
from tqdm import tqdm


stage_prefix = '02'

raw_fn = './data/00_metadata.par'

dtypes = {
        'id': 'numeric',
        'gen': 'string',
        'ssp': 'string',
        'group': 'string',
        'en': 'string',
        'rec': 'string',
        'cnt': 'string',
        'loc': 'string',
        'lat': 'numeric',
        'lng': 'numeric',
        'alt': 'numeric',
        'type': 'string',
        'sex': 'string',
        'stage': 'string',
        'method': 'string',
        'url': 'string',
        'file': 'string',
        'file-name': 'string',
        'sono': {
            'small': 'string',
            'med': 'string',
            'large': 'string',
            'full': 'string',
            },
        'osci': {
            'small': 'string',
            'med': 'string',
            'large': 'string',
            },
        'lic': 'string',
        'q': 'string',
        'length': 'string',
        'time': 'string',
        'date': 'string',
        'uploaded': 'datetime',
        'also': 'series',
        'rmk': 'string',
        'bird-seen': 'boolean',
        'animal-seen': 'boolean',
        'playback-used': 'boolean',
        'temp': 'numeric',
        'regnr': 'string',
        'auto': 'string',
        'dvc': 'string',
        'mic': 'string',
        'smp': 'string',
        }

def convert_dtypes( df ):
    df['id'] = pd.to_numeric(df['id'])
    df['gen'] =  pd.to_string(df['gen'])
    df['uploaded'] = pd.to_datetime(df['uploaded'])

def save_parquet( recordings_df, filename ):
    print(f'Saving parquet: {filename}, {recordings_df.shape}')

    print( recordings_df.shape )
    loaded_df = pd.read_parquet(filename)

    print( loaded_df.shape )
    loaded_df = pd.concat( [ loaded_df, recordings_df ] )
    loaded_df.to_parquet(filename)


def main():

    # open parquet file if it exists
    try:
        recordings_df = pd.read_parquet(raw_fn)
    except FileNotFoundError:
        recordings_df = pd.DataFrame()

    # find the latest upload date in parquet
    recordings_df

    for area in areas:
        area_data = collect_area(area)
        area_data['area'] = area
        area_data['imported'] = run_datetime

        recordings_df = pd.concat( [recordings_df, area_data] )
        save_parquet( recordings_df , recordings_fn)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)

