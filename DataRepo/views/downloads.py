import os
from collections import defaultdict
from warnings import warn

from django.conf import settings
from django.views.generic import TemplateView

from DataRepo.utils.exports_organizer import ExportParseError, ExportsOrganizer
from DataRepo.utils.file_utils import get_readable_file_size


class DownloadsView(TemplateView):
    template_name = "downloads/downloads.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        downloads_dir = settings.DOWNLOADS_DIR

        # Get all file names in the directory (ignoring subdirectories)
        files = defaultdict(list)
        if os.path.exists(downloads_dir):
            filename: str
            for filename in sorted(
                (f for f in os.listdir(downloads_dir) if f is not None),
                key=str.casefold,
            ):
                filepath = os.path.join(downloads_dir, filename)
                if not os.path.isfile(filepath) or filename.endswith(
                    ExportsOrganizer.staged_ext
                ):
                    continue

                try:
                    # This is just to validate the filename
                    ExportsOrganizer.parse_export_filename(filename)
                except ExportParseError:
                    warn(
                        f"Unsupported/unrecognized file found in the downloads directory: '{downloads_dir}': "
                        f"'{filename}'."
                    )
                    continue

                if (
                    ExportsOrganizer.alldatatypes_str in filename
                    and ExportsOrganizer.allstudies_str in filename
                ):
                    files["all"].append(
                        {"file": filename, "size": get_readable_file_size(filepath)}
                    )
                elif ExportsOrganizer.alldatatypes_str in filename:
                    files["studies"].append(
                        {"file": filename, "size": get_readable_file_size(filepath)}
                    )
                elif ExportsOrganizer.allstudies_str in filename:
                    files["datatypes"].append(
                        {"file": filename, "size": get_readable_file_size(filepath)}
                    )
                else:
                    files["individual"].append(
                        {"file": filename, "size": get_readable_file_size(filepath)}
                    )

        context["files"] = files
        context["downloads_url"] = settings.DOWNLOADS_URL

        return context
