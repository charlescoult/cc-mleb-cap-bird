import pandas as pd

# coerce_f is a function that returns the dtype from a given input
class Column:
    def __init__( self, dtype, coerce_f ):
        self.dtype = dtype
        self.coerce_f = coerce_f

dtypes = {
        'string': 'string',
        'int': 'int',
        'float': 'float',
        'list': 'list',
        'boolean': 'boolean',
        'category': 'category',
        'Timestamp': 'Timestamp',
        'Timedelta': 'Timedelta',
        }

schema = {

        # id: the catalogue number of the recording on xeno-canto
        # "id": dtypes['int'],

        # gen: the generic name of the species
        "gen": dtypes['string'],

        # sp: the specific name (epithet) of the species
        "sp": dtypes['string'],

        # ssp: the subspecies name (subspecific epithet)
        "ssp": dtypes['string'],

        # group: the group to which the species belongs (birds, grasshoppers)
        "group": dtypes['string'],

        # en: the English name of the species
        "en": dtypes['string'],

        # rec: the name of the recordist
        "rec": dtypes['string'],

        # cnt: the country where the recording was made
        "cnt": dtypes['string'],

        # loc: the name of the locality
        "loc": dtypes['string'],

        # lat: the latitude of the recording in decimal coordinates
        "lat": dtypes['float'],

        # lng: the longitude of the recording in decimal coordinates
        "lng": dtypes['float'],

        # alt: Altitude?
        "alt": dtypes['int'],

        # type: the sound type of the recording (combining both predefined terms such as 'call' or 'song' and additional free text options)
        "type": dtypes['string'],

        # sex: the sex of the animal
        "sex": dtypes['string'],

        # stage: the life stage of the animal (adult, juvenile, etc.)
        "stage": dtypes['string'],

        # method: the recording method (field recording, in the hand, etc.)
        "method": dtypes['string'],

        # url: the URL specifying the details of this recording
        "url": dtypes['string'],

        # file: the URL to the audio file
        "file": dtypes['string'],

        # file-name: the original file name of the audio file
        "file-name": dtypes['string'],

        # sono: an object with the urls to the four versions of sonograms
        # "sono": {
        #         "small": dtypes['string'],
        #         "med": dtypes['string'],
        #         "large": dtypes['string'],
        #         "full": dtypes['string'],
        #         },
        "sono.small": dtypes['string'],
        "sono.med": dtypes['string'],
        "sono.large": dtypes['string'],
        "sono.full": dtypes['string'],

        # osci: an object with the urls to the three versions of oscillograms
        # "osci": {
        #         "small": dtypes['string'],
        #         "med": dtypes['string'],
        #         "large": dtypes['string'],
        #         },

        "osci.small": dtypes['string'],
        "osci.med": dtypes['string'],
        "osci.large": dtypes['string'],

        # lic: the URL describing the license of this recording
        "lic": dtypes['string'],

        # q: the current quality rating for the recording
        "q": dtypes['string'],

        # length: the length of the recording in minutes
        "length": dtypes['Timedelta'],

        # time: the time of day that the recording was made
        "time": dtypes['Timedelta'],

        # date: the date that the recording was made
        "date": dtypes['Timestamp'],

        # uploaded: the date that the recording was uploaded to xeno-canto
        "uploaded": dtypes['Timestamp'],

        # also: an array with the identified background species in the recording
        "also": dtypes['list'],

        # rmk: additional remarks by the recordist
        "rmk": dtypes['string'],

        # bird-seen: despite the field name (which was kept to ensure backwards compatibility), this field indicates whether the recorded animal was seen
        "bird-seen": dtypes['string'],

        # animal-seen: was the recorded animal seen?
        "animal-seen": dtypes['string'],

        # playback-used: was playback used to lure the animal?
        "playback-used": dtypes['string'],

        # temperature: temperature during recording (applicable to specific groups only)
        "temp": dtypes['string'],

        # regnr: registration number of specimen (when collected)
        "regnr": dtypes['string'],

        # auto: automatic (non-supervised) recording?
        "auto": dtypes['boolean'],

        # dvc: recording device used
        # "dvc": dtypes['string'],
        "dvc": dtypes['category'],

        # mic: microphone used
        # "mic": dtypes['string'],
        "mic": dtypes['category'],

        # smp: sample rate
        "smp": dtypes['int'],

        # program defined 'area', aka continent
        'area': dtypes['category']
        }

# noob time parsing...
# I was getting errors trying to use pd.to_timedelta
# because the times weren't in an expected format and there's
# no way to specify a custom format
def parse_td( td_str ):
    td_str_split = td_str.split(':')
    try:
        try:
            seconds = int(td_str_split[-1])
        except IndexError:
            seconds = 0
        try:
            minutes = int(td_str_split[-2])
        except IndexError:
            minutes = 0
        try:
            hours = int(td_str_split[-3])
        except IndexError:
            hours = 0
    except ValueError:
        return None
    return pd.Timedelta( seconds + minutes*60 + hours*60*60 )

# I'd like to use DataFrame.astype but it is not recursive
# and can't convert empty strings to numerical data
def apply_schema( df ):

    for column, dtype in schema.items():
        if ( dtype == 'int'):
            # Question: Why the heck does to_numeric convert
            # a column full of whole number strings
            # to a column of floats when it can use ints...
            df[column] = pd.to_numeric(
                    df[column],
                    errors='coerce',
                    ).fillna(-1).astype('int')
        elif ( dtype == 'float' ):
            df[column] = pd.to_numeric(
                    df[column],
                    errors='coerce',
                    )   
        elif ( dtype == 'string' ):
            df[column] = df[column].astype( 'string' )
        elif ( dtype == 'Timestamp' ):
            df[column] = pd.to_datetime(
                    df[column],
                    errors='coerce')
        elif ( dtype == 'Timedelta' ):
            df[column] = df[column].apply( parse_td )
        elif ( dtype == 'category' ):
            df[column] = df[column].astype('category')
        elif ( dtype == 'boolean' ):
            pass

    print(df.info())

    return df


