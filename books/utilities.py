from datetime import datetime
from dateutil import tz


def get_database_name(name='goodreads', suffix='raw'):
    date_version = datetime.now(tz=tz.UTC).date()

    return f'{name}_{date_version}_{suffix}.db'


