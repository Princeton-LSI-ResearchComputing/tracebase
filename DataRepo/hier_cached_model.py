from functools import wraps
from typing import List

from django.conf import settings
from django.core.cache import cache
from django.db.models import Model

use_cache = True
caching_updates = True


def cached_function(f):
    """
    This function returns a wrapper function to be called instead of the function that has this decorator
    """

    @wraps(f)
    def get_result(self, *args, **kwargs):
        """
        This decorator function is called for any function with the @cached_function decorator.  This function decides
        whether the use the cache or call the original function and save its output in the cache before returning the
        result
        """
        result, is_cache_good = get_cache(self, f.__name__)
        if not is_cache_good:
            result = f(self, *args, **kwargs)
            set_cache(self, f.__name__, result)
        return result

    return get_result


def get_cache(rec, cache_prop_name):
    """
    Returns a cached value and a boolean as to whether the cached value was good or not (e.g. not cached)
    """
    if not use_cache:
        return None, False
    try:
        good_cache = True
        uncached = object()
        cachekey = get_cache_key(rec, cache_prop_name)
        result = cache.get(cachekey, uncached)
        if result is uncached:
            good_cache = False
    except Exception as e:
        # Allow tracebase to still work, just without caching
        print(e)
        result = None
        good_cache = False
    if settings.DEBUG:
        print(f"Returning cached {cachekey}?: {good_cache} Value: {result}")
    return result, good_cache


def set_cache(rec, cache_prop_name, value):
    """
    Caches a given value
    """
    if not use_cache or not caching_updates:
        return False
    try:
        cachekey = get_cache_key(rec, cache_prop_name)
        cache.set(cachekey, value, timeout=None, version=1)
        print(f"Setting cache {cachekey} to {value}")
    except Exception as e:
        # Allow tracebase to still work, just without caching
        print(e)
        return False
    return True


def get_cache_key(rec, cache_prop_name):
    """
    Generates a cache key given a record and the cached_property method name
    """
    return ".".join([rec.__class__.__name__, str(rec.pk), cache_prop_name])


def disable_caching_updates():
    """
    Prevents storage and deletion of cached values.  Currently only used for loading scripts.
    """
    global caching_updates
    caching_updates = False


def enable_caching_updates():
    """
    Reenables storage and deletion of cached values.  Currently only used for loading scripts.
    """
    global caching_updates
    caching_updates = True


class HierCachedModel(Model):
    # Set these in the derived class
    parent_cache_key_name: str = None
    child_cache_related_names: List[str] = []

    def save(self, *args, **kwargs):
        """
        If caching updates are enabled, trigger the deletion of every cached value under the linked Animal record
        """
        if caching_updates:
            if self.parent_cache_key_name is not None:
                parent_instance = getattr(self, self.parent_cache_key_name)
                parent_instance.delete_cache()
            else:
                self.delete_cache()
        super().save(*args, **kwargs)  # Call the "real" save() method.

    def delete(self, *args, **kwargs):
        """
        If caching updates are enabled, trigger the deletion of every cached value under the linked Animal record
        """
        if caching_updates:
            if self.parent_cache_key_name is not None:
                parent_instance = getattr(self, self.parent_cache_key_name)
                parent_instance.delete_cache()
            else:
                self.delete_cache()
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

    def delete_cache(self):
        """
        Cascading cache deletion (originally triggered through an Animal record down to PeakData
        """
        if not caching_updates:
            return
        delete_keys = []
        # For every cached property, delete the cache value
        for member_key in self.__class__.__dict__.keys():
            if (
                self.__class__.__dict__[member_key].__class__.__name__
                == "cached_property"
            ):
                cache_key = get_cache_key(self, member_key)
                if settings.DEBUG:
                    print(f"Deleting cache {cache_key}")
                delete_keys.append(cache_key)
        cache.delete_many(delete_keys)
        # For every child model for which we have a related name
        for child_rel_name in self.child_cache_related_names:
            child_instance = getattr(self, child_rel_name)
            # For every child record, call its delete_cache()
            for rec in child_instance.all():
                rec.delete_cache()

    class Meta:
        abstract = True
