import os, sys
from datetime import datetime
from datetime import timedelta
import requests
import json
import pandas as pd
from tqdm import tqdm

import dataflow

# IT WILL NOT BE EFFECIENT AND THAT IS OK.

stage_prefix = '00'
data_fn = dataflow.get_fn(stage_prefix)

# query_str='cnt:brazil'
areas = [
        'africa',
        'america',
        'asia',
        'australia',
        'europe',
        ]

# rate limit of 1 request per second - no point in making async
api_url = 'https://www.xeno-canto.org/api/2/recordings'

def convert_dtypes():
    # todo: use a schema
    df['uploaded'] = pd.to_datetime(df['uploaded'])

def query_recordings(query_str):

    query_var='?query=' + query_str
    query = api_url + query_var

    r = requests.get(query)
    r_json = r.json()
    num_pages = r_json['numPages']
    print(f'Pages: {num_pages}')
    print(f'Recordings: {r_json["numRecordings"]}')
    print(f'Species: {r_json["numSpecies"]}')

    # generate a pandas df to store recording metadata as we go
    recordings_df = pd.DataFrame(r_json['recordings'], index='id')
    recordings_df = convert_dtypes(recordings_df)

    # go through each page and compile a list of recordings
    for page_num in tqdm(range(2, num_pages + 1)):
        page_var = '&page=' + str(page_num)
        query = api_url + query_var + page_var
        r_json = requests.get(query).json()

        new_recordings_df = pd.DataFrame(r_json['recordings'], index='id')
        new_recordings_df = convert_dtypes(recordings_df)

        recordings_df = pd.concat( [
            recordings_df,
            new_recordings_df
            ] )

    return recordings_df

def collect_area( area, from_dt=datetime.min, to_dt=datetime.max ):
    print('Collecting: ' + area)
    return query_recordings(f'area:{area}')

def save_parquet( df, filename ):
    print(f'Saving parquet: {filename}, {df.shape}')

    df.to_parquet(filename)

def main():

    # open parquet file if it exists
    try:
        df = pd.read_parquet(data_fn)
        # find the latest upload date in parquet
        latest = df['uploaded'].max()
        print("Parquet file found. Latest date in parquet is: " + latest.strftime("%m/%d/%Y") )
    except FileNotFoundError:
        df = pd.DataFrame()

    for area in areas:
        area_data = collect_area(area, to_dt = latest - timedelta(day=1) )
        area_data['area'] = area

        df = pd.concat( [df, area_data] )
        save_parquet( df , data_fn)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)

