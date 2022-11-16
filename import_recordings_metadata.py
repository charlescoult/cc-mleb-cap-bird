import os, sys
import requests
import json
import pandas as pd
from tqdm import tqdm

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

    # go through each page and compile a list of recordings
    for page_num in tqdm(range(2, num_pages + 1)):
        page_var = '&page=' + str(page_num)
        query = api_url + query_var + page_var
        r_json = requests.get(query).json()
        recordings_df = pd.concat( [
            recordings_df,
            pd.DataFrame(r_json['recordings'], index='id')
            ] )

    return recordings_df

def collect_area(area):
    print('Collecting: ' + area)
    return query_recordings(f'area:{area}')

def save_parquet( recordings_df, filename ):
    print(f'Saving parquet: {filename}, {recordings_df.shape}')
    # recordings_df.to_parquet(filename)
    print( recordings_df.shape )
    loaded_df = pd.read_parquet(filename)
    print( loaded_df.shape )
    loaded_df = pd.concat( [ loaded_df, recordings_df ] )
    loaded_df.to_parquet(filename)
    # verify_df = pd.read_parquet(filename)
    # print(f'Verify parquet: {verify_df.shape}')


def main():
    recordings_df = pd.DataFrame()
    for area in areas:
        area_data = collect_area(area)
        area_data['area'] = area
        recordings_df = pd.concat( [recordings_df, area_data] )
        save_parquet( recordings_df , 'recordings_metadata.par')

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)

