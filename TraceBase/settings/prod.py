from .base import *

# Raises django's ImproperlyConfigured exception if SECRET_KEY not in os.environ
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")

# NOTE: Explicitly forcing these debug/test environment variables to false *after* importing from base.  They cannot be
# overridden by the values set in .env.  This is a security issue.
DEBUG = False
SQL_LOGGING = False
DEBUG_TOOLBAR_ENABLED = False
DEBUG_TOOLBAR = False
TESTING = False
