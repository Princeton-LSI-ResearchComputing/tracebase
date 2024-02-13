import os

import environ

# This "app"'s only purpose is to allow temporary models to be created for testing, and those models should "go away"
# after testing is complete.  All it needs is the basic settings with a configured database.  It may use the same port
# and user as the project's main account.

env = environ.Env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECRET_KEY = env("SECRET_KEY")
DEBUG = env.bool("DEBUG", default=False)
ALLOWED_HOSTS = env.list("ALLOWED_HOSTS", default=["localhost", "127.0.0.1"])

INSTALLED_APPS = [
    "loader",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("DATABASE_NAME"),
        "USER": env("DATABASE_USER"),
        "PASSWORD": env("DATABASE_PASSWORD"),
        "HOST": env("DATABASE_HOST", ""),
        "PORT": env("DATABASE_PORT", ""),
        "TEST": {
            "USER": env("DATABASE_USER"),
            "TBLSPACE": "default_test_tbls",
            "TBLSPACE_TMP": "default_test_tbls_tmp",
        },
    },
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "America/New_York"
USE_I18N = True
USE_L10N = True
USE_TZ = True
