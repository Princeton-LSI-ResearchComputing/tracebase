from django.apps import AppConfig


# README: This file is intended to be used as an initial step toward splitting out some Django apps (e.g.
# MaintainedModel, the advanced search code, and the new generic Loader code).  Said "apps" are being developed
# currently concurrently with TraceBase, so in order for the tests to run (tests for "installed 3rd party apps" don't
# run with the test suite), until those apps have their own projects, those tests must be integrated into the tracebase
# tests (as we have been doing).  And those tests rely on current tracebase models (Tissues, Compounds, etc).  As a
# first step toward splitting those apps off into their own projects, the reliance on TraceBase models must be
# extracted, so having separate "app" configs for those projects allows dyanmically created models to be used for
# testing.  And actually, when those apps are split off, they will still have no models tied to them, so when those apps
# are split into their own projects, this config will leave TraceBase.


class LoaderTestConfig(AppConfig):
    """This class is for dynamically creating models that only exist for testing.
    See:
    - https://docs.djangoproject.com/en/dev/internals/contributing/writing-code/unit-tests/#isolating-model-registration
    - https://stackoverflow.com/a/76400577/2057516
    - Example in the wild:
      - https://github.com/laymonage/django-jsonfield-backport/blob/master/tests/test_invalid_models.py
    """
    name = "DataRepo.tests.apps.loader"
    label = "loader"
