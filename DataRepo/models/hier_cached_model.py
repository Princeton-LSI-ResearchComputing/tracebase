from functools import wraps
from typing import Dict, List, Optional

from django.conf import settings
from django.core.cache import cache
from django.db.models import Model

caching_retrievals = True
caching_updates = True
throw_cache_errors = False
func_name_lists: Dict[str, List] = {}


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

    class_name = f.__qualname__.split(".")[0]
    if class_name in func_name_lists:
        func_name_lists[class_name].append(f.__name__)
    else:
        func_name_lists[class_name] = [f.__name__]
    if settings.DEBUG:
        print(f"Added cached_function decorator to function {f.__qualname__}")
    return get_result


def get_cache(rec, cache_func_name):
    """
    Returns a cached value and a boolean as to whether the cached value was good or not (e.g. not cached)
    """
    if not caching_retrievals:
        return None, False
    try:
        good_cache = True
        uncached = object()
        cachekey = get_cache_key(rec, cache_func_name)
        result = cache.get(cachekey, uncached)
        if result is uncached:
            result = None
            good_cache = False
        if settings.DEBUG:
            print(f"Getting cache {cachekey}")
    except Exception as e:
        # Allow tracebase to still work, just without caching
        print(e)
        result = None
        good_cache = False
        if throw_cache_errors:
            raise Exception(f"{rec.__class__.__name__}.{cache_func_name} ERROR: {e}")
    return result, good_cache


def set_cache(rec, cache_func_name, value):
    """
    Caches a given value
    """
    if not caching_updates:
        return False
    try:
        cachekey = get_cache_key(rec, cache_func_name)
        cache.set(cachekey, value, timeout=None, version=1)
        if settings.DEBUG:
            print(f"Setting cache {cachekey} to {value}")
        root_rec, first_method_name = rec.get_representative_root_rec_and_method()
        # If this isn't the representative, tell the root record that caches exist under it somewhere
        if (
            root_rec.__class__.__name__ != rec.__class__.__name__
            or cache_func_name != first_method_name
        ):
            # Set a single cached value in the parent to act as a representative
            rep_result, is_rep_cache_good = get_cache(root_rec, first_method_name)
            rep_cachekey = get_cache_key(root_rec, first_method_name)
            if not is_rep_cache_good:
                rep_result = getattr(root_rec, first_method_name)
                cache.set(rep_cachekey, rep_result, timeout=None, version=1)
    except Exception as e:
        # Allow tracebase to still work, just without caching
        print(e)
        if throw_cache_errors:
            raise Exception(f"{rec.__class__.__name__}.{cache_func_name} ERROR: {e}")
        return False
    return True


def get_cache_key(rec, cache_func_name):
    """
    Generates a cache key given a record and the cached_property method name
    """
    return ".".join([rec.__class__.__name__, str(rec.pk), cache_func_name])


def delete_all_caches():
    cache.clear()


def get_cached_method_names():
    """
    Returns the structure storing the cached function names.  The structure is a dict keyed on class name whose values
    are lists of the method names that are cached.
    """
    return func_name_lists


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


def disable_caching_retrievals():
    """
    Prevents storage and deletion of cached values.  Currently only used for loading scripts.
    """
    global caching_retrievals
    caching_retrievals = False


def enable_caching_retrievals():
    """
    Reenables storage and deletion of cached values.  Currently only used for loading scripts.
    """
    global caching_retrievals
    caching_retrievals = True


def disable_caching_errors():
    """
    Prevents exceptions from being thrown when retrieving or setting a cached value (so that the site works when
    caching's broken)
    """
    global throw_cache_errors
    throw_cache_errors = False


def enable_caching_errors():
    """
    Allows exceptions to be thrown when retrieving or setting cached values
    """
    global throw_cache_errors
    throw_cache_errors = True


class HierCachedModel(Model):
    """
    This class maintains caching validity for a django.models.Model class which contains functions with the
    `@cached_function` decorator (above).  Only methods that take no arguments are supported.  Caching would still work
    without this class (as long as you call set/get_cache yourself), but expiring invalid caches would be up to you.
    """

    # Set these related key names in the derived class (including the backwards relationships whose names are specified
    # in the related class). E.g. In the Sample class set the parent_related_key_name to 'animal' and the
    # child_related_key_names to ['msruns']
    parent_related_key_name: Optional[str] = None
    child_related_key_names: Optional[List[str]] = []

    def save(self, *args, **kwargs):
        """
        If caching updates are enabled, trigger the deletion of every cached value under the linked Animal record
        """
        if caching_updates:
            self.delete_related_caches()
        super().save(*args, **kwargs)  # Call the "real" save() method.

    def delete(self, *args, **kwargs):
        """
        If caching updates are enabled, trigger the deletion of every cached value under the linked Animal record
        """
        if caching_updates:
            self.delete_related_caches()
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

    def delete_related_caches(self):
        """
        If caching updates are enabled, trigger the deletion of every cached value under the linked Animal record
        """
        if caching_updates:
            self.get_root_record().delete_descendant_caches()

    def delete_descendant_caches(self):
        """
        Cascading cache deletion from self, downward. Call from a root record to delete all belonging to the same root
        parent
        """
        if not caching_updates:
            return
        delete_keys = []
        # For every cached property, delete the cache value
        for cached_function in self.get_my_cached_method_names():
            cache_key = get_cache_key(self, cached_function)
            if settings.DEBUG:
                print(f"Deleting cache {cache_key}")
            delete_keys.append(cache_key)
        if len(delete_keys) > 0:
            cache.delete_many(delete_keys)
        # For every child model for which we have a related name
        for child_rel_name in self.child_related_key_names:
            child_instance = getattr(self, child_rel_name)
            # For every child record, call its delete_descendant_caches()
            for rec in child_instance.all():
                rec.delete_descendant_caches()

    @classmethod
    def get_my_cached_method_names(cls):
        """
        Convenience method to retrieve all the cached functions of the calling model.
        """
        if cls.__name__ in func_name_lists:
            return func_name_lists[cls.__name__]
        else:
            if settings.DEBUG:
                print(f"Class [{cls.__name__}] does not have any cached functions.")
            return []

    def caches_exist(self):
        """
        Uses the first cached method of the root model record, to which the calling record belongs, to infer whether
        *any* cached value exists under the root model record (e.g. the specific Animal).  When inserting data into the
        database, call this method from any HierCachedModel record to know whether the insert of this record indicates
        whether the caches under the root record should all be expired.
        """
        root_rec, first_method_name = self.get_representative_root_rec_and_method()
        result, is_cache_good = get_cache(root_rec, first_method_name)
        return is_cache_good

    def set_caches_exist(self):
        """
        This method should be called whenever any value is saved in the cache so that the root model knows whether any
        cached values exist under it.  There is no way to infer a false value because if you did so by deleting the
        cache of the representative record, that doesn't delete the caches under it - and then if another cached value
        under it sets it back to true, that vestigial (presumably) invalid value would persist, so the only way to set
        the value to false is to delete an entire hierarchy of cached values.  delete_descendant_caches is called when
        a record's save or delete method is called (i.e. when something in the database changes).
        """
        root_rec, first_method_name = self.get_representative_root_rec_and_method()
        # Set a single cached value in the parent to act as a representative
        result, is_cache_good = get_cache(root_rec, first_method_name)
        if not is_cache_good:
            result, is_cache_good = get_cache(root_rec, first_method_name)
            status = set_cache(root_rec, first_method_name, result)
        else:
            status = is_cache_good
        return status

    def get_representative_root_rec_and_method(self):
        """
        From any record in the hierarchy, retrieve the root record and its representative cached_method name (used to
        infer that caches under it exist somewhere, when deciding whether or not to go through the lengthy process of
        deleteing the caches associated with the root).
        """
        root_rec = self.get_root_record()
        method_names = root_rec.get_my_cached_method_names()
        if len(method_names) > 0:
            return root_rec, method_names[0]
        else:
            raise Exception(
                f"The root model [{root_rec.__class__.__name__}] must contain at least 1 cached function in order to "
                "maintain hierarchical cached values."
            )

    def get_root_record(self):
        """
        From any record in the hierarchy, obtain the root record it is associated with.
        """
        if self.parent_related_key_name is not None:
            parent_instance = getattr(self, self.parent_related_key_name)
            return parent_instance.get_root_record()
        else:
            return self

    class Meta:
        abstract = True
