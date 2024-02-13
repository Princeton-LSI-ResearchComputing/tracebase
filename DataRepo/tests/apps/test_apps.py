from django.apps import AppConfig

# PURPOSE:
# The directory in which this resides ("apps") is for creating an "app" that only exists for testing purposes.  It
# facilitates the need to test various portable generic features that deserve (but have not yet been implemented as)
# their own app.  It allows you to implement tests that do not rely on code specific to the surrounding app/project,
# e.g. to create 1-off models just for testing.
#
# USAGE:
# 1. Add an app folder, naming it whatever you want (to identify the app), to "apps" in this directory.
# 2. Add a config class below that inherits from AppConfig & name it using a python path ending in the app folder name.
# 3. Add a python path ending in the cklass name you created below to INSTALLED_APPS in the project's settings.py file.
# 4. Add a new settings.py file to the app folder in this directory with what you need for the tests (e.g. DB settings).
# 5. So that what you do in the tests do not persist after testing, import isolate_apps and decorate every test class
#    that uses your "app" with it, supplying the python path to your app folder created in step 1, e.g.:
#
#        from django.test.utils import isolate_apps
#
#        @isolate_apps("DataRepo.tests.apps.loader")
#        class MyTestClass(TestCase):
#            pass
#
# REFERENCES:
# - https://docs.djangoproject.com/en/dev/internals/contributing/writing-code/unit-tests/#isolating-model-registration
# - https://stackoverflow.com/a/76400577/2057516
# - Example in the wild:
#   - https://github.com/laymonage/django-jsonfield-backport/blob/master/tests/test_invalid_models.py


class LoaderTestConfig(AppConfig):
    """This class is for dynamically creating models that only exist during testing."""

    name = "DataRepo.tests.apps.loader"
    label = "loader"
