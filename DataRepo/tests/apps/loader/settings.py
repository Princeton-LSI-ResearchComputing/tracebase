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
    "DataRepo.tests.apps.test_apps.LoaderTestConfig",
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

# Added this while resolving failed test runs in #1713. The individual tests worked, but Django's system checks raised
# error E336 and no tests would run.  This is because I'd added a M:M field in a temporary test model, which requires
# the model to exist in the DB - but that's expressly what using isolate_apps does: it prevents the models from being
# created in the DB, and apparently, I had misunderstood the point of this loader test app for dynamically created
# models.  Its supposed to not support record creation or DB operations at all.  So I don't know how exactly this ever
# worked, because that's what I was doing.  From what I can discern from ChatGPT's help is that this app wasn't really
# working.  Models were kind of luckily being created.
SILENCED_SYSTEM_CHECKS = ["fields.E336"]
