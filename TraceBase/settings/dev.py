import sys

from .base import *

SECRET_KEY = env("SECRET_KEY", default="unsafe-secret-key")

# SECURITY WARNING: don't run with debug turned on in production!
# NOTE: If you want to test what you would see in production when DEBUG=False, you must start the server with:
#     python manage.py runserver --insecure
# because runserver will not load static files without it (whereas in a production environment, the web server would
# serve those files).  See https://stackoverflow.com/a/5836728/2057516
DEBUG = env.bool("DEBUG", default=True)

# Logging settings
# NOTE: to print SQL, DEBUG must be True, and to print SQL during a particular test, each test method must be decorated
# with: `@override_settings(DEBUG=True)`
SQL_LOGGING = env.bool("SQL_LOGGING", default=False)
if SQL_LOGGING is True:
    LOGGING = {
        "version": 1,
        "filters": {
            "require_debug_true": {
                "()": "django.utils.log.RequireDebugTrue",
            }
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "filters": ["require_debug_true"],
                "class": "logging.StreamHandler",
            }
        },
        "loggers": {
            "django.db.backends": {
                "level": "DEBUG",
                "handlers": ["console"],
            }
        },
    }

# See: django-debug-toolbar.readthedocs.io/en/latest/installation.html#disable-the-toolbar-when-running-tests-optional
DEBUG_TOOLBAR_ENABLED = False
DEBUG_TOOLBAR = env.bool("DEBUG_TOOLBAR", default=True)
if DEBUG_TOOLBAR is True:
    TESTING = "test" in sys.argv
    try:
        import debug_toolbar  # noqa: F401 # pylint: disable=unused-import

        DEBUG_TOOLBAR_INSTALLED = True
    except ImportError:
        DEBUG_TOOLBAR_INSTALLED = False

    if (
        DEBUG
        and not TESTING
        and DEBUG_TOOLBAR_INSTALLED
        # Static files are configured to debug_toolbar's requirements
        and "django.contrib.staticfiles" in INSTALLED_APPS
        and STATIC_URL == "static/"
        # Templates are configured to debug_toolbar's requirements
        and any(
            [
                template["BACKEND"] == "django.template.backends.django.DjangoTemplates"
                and template["APP_DIRS"] is True
                for template in TEMPLATES
            ]
        )
    ):
        # On the dev site, you need to run `python manage.py collectstatic` to be able to use the toolbar
        # NOTE: Running collectstatic puts the aggregated static files in tracebase/static.  After running it, (which
        # you should only need to do once), run `mv TraceBase/static static`.
        PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
        STATIC_ROOT = os.path.join(PROJECT_DIR, "static")

        DEBUG_TOOLBAR_ENABLED = True
        INSTALLED_APPS.append("debug_toolbar")
        # See https://django-debug-toolbar.readthedocs.io/en/latest/installation.html#add-the-middleware
        MIDDLEWARE.insert(0, "debug_toolbar.middleware.DebugToolbarMiddleware")
        INTERNAL_IPS = ALLOWED_HOSTS[:]
        # Override the debug toolbar's logic to decide whether to run or not (we're using the conditional logic above)
        DEBUG_TOOLBAR_CONFIG = {
            "SHOW_TOOLBAR_CALLBACK": lambda _: True,
            "SHOW_COLLAPSED": True,
            "SQL_WARNING_THRESHOLD": 70,
        }
