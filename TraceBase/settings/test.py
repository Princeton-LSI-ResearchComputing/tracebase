import os
from copy import deepcopy
from typing import Dict

from .dev import *

INSTALLED_APPS.extend(
    [
        "DataRepo.tests.apps.test_apps.LoaderTestConfig",
    ]
)

# NOTE: TEST_MEDIA_ROOT is used in DataRepo/tests/tracebase_test_case.py
TEST_MEDIA_ROOT = env.str(
    "TEST_ARCHIVE_DIR", default=os.path.join(BASE_DIR, "archive_test")
)

DEFAULT_STORAGES = deepcopy(STORAGES)

# NOTE: TEST_STORAGES is used in TraceBase/runner.py
TEST_STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.InMemoryStorage",
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}

# NOTE: TEST_FILE_STORAGES is used in DataRepo/tests/tracebase_test_case.py
TEST_FILE_STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}

# NOTE: TEST_CACHES is used in various tests as an argument to @override_settings()
TEST_CACHES: Dict[str, Dict[str, object]] = {
    "default": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "tracebase_cache_table",
        "TIMEOUT": 1200,
        "OPTIONS": {"MAX_ENTRIES": 1000},
        "KEY_PREFIX": "TEST",
    }
}

# NOTE: CACHES affects the usage of @cached_properties
# See: https://docs.djangoproject.com/en/dev/topics/cache/#setting-up-the-cache
CACHES_SETTING = env.str("CACHES", default="PROD_CACHES")
if CACHES_SETTING == "TEST_CACHES":
    CACHES = TEST_CACHES
elif CACHES_SETTING != "PROD_CACHES":
    print(
        f"Invalid CACHE_SETTINGS value: {CACHES_SETTING} in .env. Defaulting to PROD_CACHES. Valid values are "
        "TEST_CACHES and PROD_CACHES."
    )

# Define a custom test runner
# https://docs.djangoproject.com/en/4.2/topics/testing/advanced/#using-different-testing-frameworks
# NOTE: This is implicitly used by TraceBase/runner.py
TEST_RUNNER = "TraceBase.runner.TraceBaseTestSuiteRunner"
