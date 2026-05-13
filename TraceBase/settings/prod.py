from .base import *

# Raises django's ImproperlyConfigured exception if SECRET_KEY not in os.environ
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")

# NOTE: Explicitly setting DEBUG to false.  This is a security issue.  See dev.py for controlling DEBUG with .env.
DEBUG = False
SQL_LOGGING = False
DEBUG_TOOLBAR_ENABLED = False
DEBUG_TOOLBAR = False
