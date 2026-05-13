from .dev import *  # noqa: F401

# The above is a backward-compatibility import.  This makes it possible for the dev site to work while remaining
# compatible with old branches.  I.e. Both old and new branches are able to work on the dev site in DEBUG mode by
# setting DJANGO_SETTINGS_MODULE in the system environment settings that apache picks up and passes to wsgi.py.
