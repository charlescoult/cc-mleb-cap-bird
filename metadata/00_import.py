import os, sys
import datetime
import requests
import json
import pandas as pd
from tqdm import tqdm

stage_prefix = '00'

# query_str='cnt:brazil'
areas = [
        'africa',
        'america',
        'asia',
        'australia',
        'europe',
        ]

parquet_fn = 'recordings_metadata.par'

# rate limit of 1 request per second - no point in making async
api_url = 'https://www.xeno-canto.org/api/2/recordings'

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

def collect_area(area):
    print('Collecting: ' + area)
    return query_recordings(f'area:{area}')

def save_parquet( recordings_df, filename ):
    print(f'Saving parquet: {filename}, {recordings_df.shape}')

    print( recordings_df.shape )
    loaded_df = pd.read_parquet(filename)

    print( loaded_df.shape )
    loaded_df = pd.concat( [ loaded_df, recordings_df ] )
    loaded_df.to_parquet(filename)


def main():

    run_datetime = datetime.now()

    # open parquet file if it exists
    try:
        recordings_df = pd.read_parquet(recordings_fn)
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

