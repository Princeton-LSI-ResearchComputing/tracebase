from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.studies_exporter import (
    BadQueryTerm,
    DuplicateSlugifiedStudyNames,
)


class StudiesExporterMainTests(TracebaseTestCase):
    def test_bad_query_term(self):
        bqt = BadQueryTerm({"bad term": ValueError("Record not found")})
        self.assertIn(
            "No study name or ID matches the provided search term(s)", str(bqt)
        )
        self.assertIn("bad term: ValueError: Record not found", str(bqt))
        self.assertIn(
            "Scroll up to see tracebacks above for each individual exception", str(bqt)
        )

    def test_duplicate_slugified_study_names(self):
        dssn = DuplicateSlugifiedStudyNames({"Dupe Study Name": 2})
        self.assertIn(
            "These slugified study names are not unique: ['Dupe Study Name']", str(dssn)
        )
