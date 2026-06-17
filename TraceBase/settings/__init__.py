from .dev import *  # noqa: F401

# TODO: The import from .dev is temporary.  See ticket: https://princeton-university.atlassian.net/browse/GREATS-304 for
# the plan for its removal.  The import is a backward-compatibility shim.  It makes it possible for the dev site to work
# while remaining compatible with old branches.  I.e. Both old and new branches can work on the dev site in DEBUG mode
# by setting DJANGO_SETTINGS_MODULE in the system environment settings that apache picks up and passes to wsgi.py.
