from warnings import warn

from django.conf import settings
from django.db import connection


def dump_cache_table_keys():
    """This is a debugging function to see what is in the cache table regardless of prefix.

    Args:
        None
    Exceptions:
        None
    Returns:
        settings.CACHES (dict): The CACHES settings.
        cache_keys_and_expires (List[Tuple[str, datetime]]): A list of cache_key and expires values from the cache table
            (regardless of the cache prefix in settings).
    """
    table_name = settings.CACHES["default"]["LOCATION"]

    with connection.cursor() as cursor:
        sql = f"SELECT cache_key, expires FROM {table_name};"
        cursor.execute(sql)
        cache_keys_and_expires = cursor.fetchall()

    return settings.CACHES, cache_keys_and_expires


def get_cache_table_size():
    """Returns the number of rows from the cache table that match the CACHE settings prefix.

    Args:
        None
    Exceptions:
        None
    Returns:
        count (int): The number of rows containing the cache settings prefix.
    """
    table_name = settings.CACHES["default"]["LOCATION"]
    max_entries = settings.CACHES["default"]["OPTIONS"]["MAX_ENTRIES"]
    prefix = settings.CACHES["default"]["KEY_PREFIX"]

    with connection.cursor() as cursor:
        sql = f"SELECT count(*) FROM {table_name} WHERE cache_key like '{prefix}:%';"
        cursor.execute(sql)
        row = cursor.fetchone()
        count = int(row[0])

    if count / max_entries >= 0.8:
        pcnt = int(count / max_entries * 100)
        warn(
            f"Cache table {table_name} is {pcnt}% full (there are {count} entries out of a max of {max_entries} "
            "allowed entries).  The caching strategy is persistant and values are updated only when they change.  "
            "Increase environment variable CACHE_MAX_ENTRIES."
        )

    return count
