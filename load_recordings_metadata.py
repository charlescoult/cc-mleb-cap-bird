import pandas as pd

recordings_metadata_parquet_filename = 'recordings_metadata.par'

rec_metadata_df = pd.read_parquet(recordings_metadata_parquet_filename)

print(rec_metadata_df.shape)


