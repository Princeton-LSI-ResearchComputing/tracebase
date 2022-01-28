"""
Django settings for TraceBase project.

Generated by 'django-admin startproject' using Django 3.1.6.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.1/ref/settings/
"""

import os
from pathlib import Path
from typing import Dict

import environ

env = environ.Env()
# reading .env file
environ.Env.read_env()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

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

ALLOWED_HOSTS = ["localhost", "127.0.0.1"]
if env("HOST", default=None):
    ALLOWED_HOSTS.append(env("HOST"))

# Application definition

INSTALLED_APPS = [
    "DataRepo.apps.DatarepoConfig",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
]

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
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases

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

VALIDATION_ENABLED = False
# If the validation database is configured in the .env file...
if env("VALIDATION_DATABASE_NAME"):
    try:
        DATABASES["validation"] = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": env("VALIDATION_DATABASE_NAME"),
            "USER": env("VALIDATION_DATABASE_USER"),
            "PASSWORD": env("VALIDATION_DATABASE_PASSWORD"),
            "HOST": env("VALIDATION_DATABASE_HOST"),
            "PORT": env("VALIDATION_DATABASE_PORT"),
        }
        VALIDATION_ENABLED = True
    except Exception as e:
        print(f"Could not configure access to the {env('VALIDATION_DATABASE_NAME')} database: {e}")


# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

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
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "America/New_York"

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.1/howto/static-files/

STATIC_URL = "/static/"

STATICFILES_DIRS = [os.path.join(BASE_DIR, "static")]


# Data submission and validation settings

# https://stackoverflow.com/questions/38345977/filefield-force-using-temporaryuploadedfile
# Added to make the validate_submission.html form work.  Could not figure out how to specify this handler for
# individual fields.  This avoids files using the InMemoryUploadedFile, which the load script complains about.
FILE_UPLOAD_HANDLERS = [
    "django.core.files.uploadhandler.TemporaryFileUploadHandler",
]
DATA_SUBMISSION_URL = "https://forms.gle/Jyp94aiGmhBNLZh6A"
DATA_SUBMISSION_EMAIL = "csgenome@princeton.edu"

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

# Logging settings
# This logging level was added to show the number of SQL queries in the server console
# Left this commented code here to prompt a conversation about how we should control this debug mode activation
# - probably via an environment setting

# LOGGING = {
#    "version": 1,
#    "filters": {
#        "require_debug_true": {
#            "()": "django.utils.log.RequireDebugTrue",
#        }
#    },
#    "handlers": {
#        "console": {
#            "level": "DEBUG",
#            "filters": ["require_debug_true"],
#            "class": "logging.StreamHandler",
#        }
#    },
#    "loggers": {
#        "django.db.backends": {
#            "level": "DEBUG",
#            "handlers": ["console"],
#        }
#    },
# }
