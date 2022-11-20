import os, sys
from datetime import datetime
from datetime import timedelta
import requests
import json
import pandas as pd
from tqdm import tqdm

import dataflow

# IT WILL NOT BE EFFICIENT AND THAT IS OK.

# # API data inconsistencies:
# Some recordings will be considered a part of multiple areas (continents)

# >>> df.loc[['524398']]
#               gen        sp ssp  group               en        rec               cnt  ... temp regnr auto dvc mic    smp       area
# id                                                                                    ...
# 524398  Casuarius  bennetti      birds  Dwarf Cassowary  Todd Mark  Papua New Guinea  ...              no          48000       asia
# 524398  Casuarius  bennetti      birds  Dwarf Cassowary  Todd Mark  Papua New Guinea  ...              no          48000  australia


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

def convert_dtypes( df ):
    # todo: use a schema
    df['uploaded'] = pd.to_datetime(df['uploaded'])

    return df

# makes a cleanly formatted df from a query result
def make_recs_df( recs_list ):

    # recs_df = pd.DataFrame( recs_list )
    # Use pd.json_normalize instead of DataFrame() in order
    # to flatten nested dicts
    recs_df = pd.json_normalize( recs_list )

    # set up index to be id as an int
    recs_df['id'] = recs_df['id'].astype(int)
    recs_df = recs_df.set_index('id')

    # convert any other types according to schema
    recs_df = convert_dtypes( recs_df )

    # because this is comming from a query body, there should be no duplicate id
    # otherwise API has unexpected behaviour that needs to be looked into
    assert not recs_df.index.has_duplicates, f"Error generating df: source list has duplicate ids"

    return recs_df

# combines (concats) dfs and drops duplicate ids
def combine_recs_dfs( recs_list ):

    df = pd.concat( recs_list )

    # drop any duplicate ids (records listed in multiple 'area's)
    df = df[ ~ df.index.duplicated() ]
    # assert not df.index.has_duplicates

    return df

def call_API( query ):
    # query and parse text result to json
    res = requests.get(query).json()
    # assert no error returned
    assert 'error' not in res, f"API call error: {res['error']} - {res['message']}"
    return res

def query_recordings(query_str):

    query_var='?query=' + query_str

    query = api_url + query_var
    print(query)

    res = call_API( query )

    num_pages = res['numPages']

    print(f'Pages: {num_pages}')
    print(f'Recordings: {res["numRecordings"]}')
    print(f'Species: {res["numSpecies"]}')

    # generate a pandas df to store recording metadata as we go

    recs_df = make_recs_df( res['recordings'] )

    # go through each page and compile a list of recordings
    for page_num in tqdm(range(2, num_pages + 1), unit='page'):
        page_var = '&page=' + str(page_num)
        query = api_url + query_var + page_var

        res = call_API( query )

        new_recs_df = make_recs_df( res['recordings'] )

        recs_df = combine_recs_dfs( [ recs_df, new_recs_df ] )

    return recs_df

def collect_area( area, since_dt = None ):
    print()
    print('Collecting: ' + area)

    query_params = f'area:{area}' 

    # for some reason adding this to query returns 0 results
    # query_params += '%20group:birds'

    # for testing:
    # since_dt = pd.Timestamp.now() - pd.Timedelta( days = 300 )

    if since_dt:
        query_params += '%20since:' + since_dt.strftime("%Y-%m-%d")

    return query_recordings(query_params)

def save_parquet( df, filename ):
    print(f'Saving parquet: {filename}, {df.shape}')

    # remove any non-birds (grasshoppers)
    df = df[df['group'] == 'birds']

    df.to_parquet(filename)

def main():

    # open parquet file if it exists
    try:
        df = pd.read_parquet(data_fn)
        # find the latest upload date in parquet
        latest = df['uploaded'].max()
        print("Parquet file found.")
        print("Latest date in parquet is: " + latest.strftime("%m/%d/%Y") )
        # start search 1 day before latest date
        start_dt = latest - pd.Timedelta( days = 1 )

    except FileNotFoundError:
        df = pd.DataFrame()
        start_dt = None

    for area in areas:
        area_df = collect_area( area, start_dt )
        area_df['area'] = area

        # combine dfs and save progress
        df = combine_recs_dfs([ df, area_df ])
        save_parquet( df , data_fn)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nProgram, interrupted.')
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)

