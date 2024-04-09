"""
Django settings for TraceBase project.

Generated by 'django-admin startproject' using Django 3.2.6.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""

import os
from pathlib import Path, PureWindowsPath
from typing import Dict

import environ

env = environ.Env()
# reading .env file
environ.Env.read_env()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# Raises django's ImproperlyConfigured exception if SECRET_KEY not in os.environ
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY", default="unsafe-secret-key")

# SECURITY WARNING: don't run with debug turned on in production!
# DEBUG=False WARNING: If you want to test what you would see in production when DEBUG=False, you must start the server
# with:
#     python manage.py runserver --insecure
# because runserver will not load static files without it (whereas in a production environment, the web server would
# serve those files).  See https://stackoverflow.com/a/5836728/2057516
DEBUG = env.bool("DEBUG", default=False)

ALLOWED_HOSTS = env.list("ALLOWED_HOSTS", default=["localhost", "127.0.0.1"])

# Application definition

INSTALLED_APPS = [
    "DataRepo.apps.DatarepoConfig",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "DataRepo.tests.apps.test_apps.LoaderTestConfig",
]

CUSTOM_INSTALLED_APPS = env.list("CUSTOM_INSTALLED_APPS", default=None)
if CUSTOM_INSTALLED_APPS:
    INSTALLED_APPS.extend(CUSTOM_INSTALLED_APPS)

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "TraceBase.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "DataRepo.context_processors.debug",
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "TraceBase.wsgi.application"


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("DATABASE_NAME"),
        "USER": env("DATABASE_USER"),
        "PASSWORD": env("DATABASE_PASSWORD"),
        "HOST": env("DATABASE_HOST"),
        "PORT": env("DATABASE_PORT"),
    }
}

# This enables the validation view
VALIDATION_ENABLED = env.bool("VALIDATION_ENABLED", default=True)

# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "America/New_York"

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/
STATIC_URL = "/static/"
STATICFILES_DIRS = [os.path.join(BASE_DIR, "static")]

# File storage location
MEDIA_URL = "/archive/"
MEDIA_ROOT = env.str("ARCHIVE_DIR", default=os.path.join(BASE_DIR, "archive"))

STORAGES = {
    # Django defaults:
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
    # Testing/production
    "testing": {
        "BACKEND": "django.core.files.storage.InMemoryStorage",
    },
    "production": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
}

# Data submission and validation settings

# https://stackoverflow.com/questions/38345977/filefield-force-using-temporaryuploadedfile
# Added to make the validate_submission.html form work.  Could not figure out how to specify this handler for
# individual fields.  This avoids files using the InMemoryUploadedFile, which the load script complains about.
FILE_UPLOAD_HANDLERS = [
    "django.core.files.uploadhandler.TemporaryFileUploadHandler",
]
SUBMISSION_FORM_URL = env.str(
    "SUBMISSION_FORM_URL", default="https://forms.gle/Jyp94aiGmhBNLZh6A"
)
FEEDBACK_URL = env.str("FEEDBACK_URL", default="https://forms.gle/LNk4kk6RJKZWM6za9")
SUBMISSION_DOC_URL = env.str(
    "SUBMISSION_DOC_URL",
    default="https://princeton-lsi-researchcomputing.github.io/tracebase/Upload/How%20to%20Upload/",
)
SUBMISSION_DOC_NAME = env.str(
    "SUBMISSION_DOC_NAME", default="TraceBase upload documentation"
)
SUBMISSION_DRIVE_DOC_URL = env.str(
    "SUBMISSION_DRIVE_DOC_URL",
    default="https://lsidocs.princeton.edu/index.php/MSDATA_Users",
)
SUBMISSION_DRIVE_TYPE = env.str("SUBMISSION_DRIVE_TYPE", default="MS Data Share")
SUBMISSION_DRIVE_FOLDER = env.str(
    "SUBMISSION_DRIVE_FOLDER",
    default=PureWindowsPath("gen-iota-cifs", "msdata", "tracebase-submissions"),
)

# Set up caching used by model cached_properties
# See: https://docs.djangoproject.com/en/dev/topics/cache/#setting-up-the-cache
PROD_CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "tracebase_cache_table",
        "TIMEOUT": None,
        "OPTIONS": {"MAX_ENTRIES": 1500000},
        "KEY_PREFIX": "PROD",
    }
}

TEST_CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "tracebase_cache_table",
        "TIMEOUT": 1200,
        "OPTIONS": {"MAX_ENTRIES": 1000},
        "KEY_PREFIX": "TEST",
    }
}

CACHES_SETTING = env.str("CACHES", default="PROD_CACHES")

CACHES: Dict[str, Dict] = PROD_CACHES
if CACHES_SETTING == "TEST_CACHES":
    CACHES = TEST_CACHES
elif CACHES_SETTING != "PROD_CACHES":
    print(
        f"Invalid CACHE_SETTINGS value: {CACHES_SETTING} in .env. Defaulting to PROD_CACHES. Valid values are "
        "TEST_CACHES and PROD_CACHES."
    )

# Define a custom test runner
# https://docs.djangoproject.com/en/4.2/topics/testing/advanced/#using-different-testing-frameworks
TEST_RUNNER = "TraceBase.runner.TraceBaseTestSuiteRunner"

# Logging settings
# Note, to print SQL, DEBUG must be True, and to print SQL during a particular test, each test method must be decorated
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
