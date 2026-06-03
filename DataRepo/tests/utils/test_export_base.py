from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.export_base import ExportBase


class ExportsOrganizerTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Study.objects.create(
            id=0,
            name="Test Study-1",
            description="Test Description",
        )
        Study.objects.create(
            id=1,
            name="Test Study 2",
            description="Test Description",
        )

    def test_get_slugified_study_names_dict(self):
        export_base = ExportBase()
        self.assertEqual(
            {0: "Test_Study_1", 1: "Test_Study_2"},
            export_base.slugified_study_names,
        )
