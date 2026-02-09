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
# REQUIREMENTS:
# Note that if you use django 3.2, you will get psycopg2 SQL errors with respect to the temporary models when you run
# any tests that use them.
#
# - django>=4.2
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


# TODO: Fix this strategy for dynamically created test tables/models.
# The models created in TableLoaderTests.generate_test_related_models started generating a Django E336 error in system
# checks, causing none of the tests to even run, when I added a ManyToManyField to a new model to test the bug fix in PR
# #1713.
# isolate_apps is not what I want.  It only "registers" models.  It never actually creates tables in the database.
# According to ChatGPT, after finally getting the tests of the TestConnectedModel to work when the whole suite is run
# (and after fixing the test app, I learned how to create real tables on the fly.  I think the only reason it was
# working before was because my test app wasn't configured correctly.  Here's what it says I need to do:
# 1. @isolate_apps is removed because we actually need real tables.
# 2. connection.schema_editor() creates the tables on the fly in the test database. They will be automatically
#    destroyed at the end of the test case.
# 3. All model references (M2M, FK) use the actual model classes — no string references like
#    "loader.TestRelatedModel" — to avoid import issues.
# 4. APP_LABEL = "loader" ensures the dynamic models are grouped under a consistent app label.
# THIS HAS BEEN IMPLEMENTED IN TableLoaderTests.generate_test_related_models.  Refer to it for an example.  It still
# needs the isolate_apps decorator on the class, to work.  Otherwise, it causes this error:
# django.core.exceptions.FieldDoesNotExist: TestConnectedModel_manyfield has no field named 'None'
# So I'm not sure how involved it will be to try and follow ChatGPT's guidance to not use isolate_apps.
#
# Initially, as I explored the E336 error, ChatGPT recommended various options to silence the error, but that was all
# for naught, because model created under isolate_apps never should create a database table.  According to ChatGPT, the
# isolate_apps decorator only works on the test methods.  When I had these calls in the class __init__ method, that is
# considered "outside" the isolation, so it auto-created the through model outside isolation (without the foreign key
# fields defined), but when the models are created, it sees the through model and thinks it's custom and complains about
# the missing foreign key fields.  Instead of putting the calls here, the other options are to:
# 1. Disable to system check for dynamic test models (this is common for dynamic test models):
#     @override_settings(
#         SILENCED_SYSTEM_CHECKS=["fields.E336"]
#     )
#     class TableLoaderTests(TracebaseTestCase):
# 2. Skip all checks (inadvisable): python manage.py test --skip-checks
# 3. Move the models into a real test app (which is what I thought I had)
#     If these models are stable and reused:
#     1. Create a real Django app (e.g. tests_loader)
#     2. Put the models in models.py
#     3. Add the app to INSTALLED_APPS during tests
#     This avoids:
#     - dynamic registration
#     - system-check conflicts
#     - M2M edge cases
#     This is the most “Django-native” solution, but more work.
# 4. Explicitly mark the M2M as unmanaged
#     This can suppress the check in some versions:
#         class Meta:
#             app_label = "loader"
#             managed = False
#     but it’s brittle and not guaranteed to suppress E336 across versions.
