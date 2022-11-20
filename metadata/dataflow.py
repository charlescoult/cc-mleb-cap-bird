
data_folder = 'data/'

stage_prefixes = [
        '00',
        '02',
        '05',
        ]

def get_fn( prefix ):
    return data_folder + prefix + '_data.parquet'

def get_prev_fn( prefix ):
    return get_fn( stage_prefixes[stage_prefixes.index(prefix) - 1] )


