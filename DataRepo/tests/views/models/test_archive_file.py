from django.urls import reverse

from DataRepo.models import ArchiveFile
from DataRepo.tests.views.models.base import (
    ModelViewTests,
    create_null_tolerance_test_class,
)


class ArchiveFileViewTests(ModelViewTests):
    def test_archive_file_list(self):
        response = self.client.get(reverse("archive_file_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/archive_file_list.html")
        # 2 mzXML's, 2 raw, and 2 peak annotation files
        self.assertEqual(6, len(response.context["archive_file_list"]))

    def test_archive_file_detail(self):
        af1 = ArchiveFile.objects.filter(
            filename="small_obob_maven_6eaas_inf.xlsx"
        ).get()
        response = self.client.get(reverse("archive_file_detail", args=[af1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/archive_file_detail.html")
        self.assertEqual(
            response.context["archivefile"].filename, "small_obob_maven_6eaas_inf.xlsx"
        )

    def test_archive_file_detail_404(self):
        af = ArchiveFile.objects.order_by("id").last()
        response = self.client.get(reverse("archive_file_detail", args=[af.id + 1]))
        self.assertEqual(response.status_code, 404)


# This runs the above tests again with auto-update diabled
ArchiveFileViewNullToleranceTests = create_null_tolerance_test_class(
    ArchiveFileViewTests
)
