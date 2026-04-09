import os
import tempfile
from datetime import datetime
from typing import Callable, Iterator

from django.db.models import Q
from django.template.defaultfilters import slugify
from django.template.loader import get_template

from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import Study
from DataRepo.utils.exceptions import AggregatedErrors, trace
from DataRepo.views.search.download import (
    AdvancedSearchDownloadMzxmlZIPView,
    AdvancedSearchDownloadView,
)


class StudiesExporter:
    sg = SearchGroup()
    all_data_types = [fmtobj.name for fmtobj in sg.modeldata.values()]
    all_zipped_data_types = ["mzXML"]
    header_template = get_template("search/downloads/download_header.tsv")
    row_template = get_template("search/downloads/download_row.tsv")

    def __init__(
        self,
        outdir,
        study_targets=None,
        data_types=None,
        overwrite=False,
    ):
        self.bad_searches = {}

        if isinstance(data_types, str):
            data_types = [data_types]
        if isinstance(study_targets, str):
            study_targets = [study_targets]

        self.outdir = outdir
        self.study_targets = study_targets or []
        self.data_types = data_types or self.all_data_types
        self.zipped_data_types = [
            dt for dt in self.data_types if dt in self.all_zipped_data_types
        ]
        self.overwrite = overwrite

    def export(self):
        # For individual traceback prints
        aes = AggregatedErrors()

        # Export time for the outfile headers
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        # Identify the study records to export (by name)
        study_ids = []
        if len(self.study_targets) > 0:
            for study_target in self.study_targets:
                # Always check for name match
                or_query = Q(name__iexact=str(study_target))
                # If the value looks like an ID
                if study_target.isdigit():
                    or_query |= Q(id__exact=int(study_target))

                try:
                    # Perform a `get` for each record so that non-matching values will raise an exception
                    study_rec = Study.objects.get(or_query)
                    study_ids.append(study_rec.id)
                except Exception as e:
                    # Buffering exception to just print the traceback
                    aes.buffer_error(e)
                    # Collect the exceptions for an easier to debug and more succinct exception to raise
                    self.bad_searches[study_target] = e

            # Summarize the encountered query issues with useful info not provided in the original exceptions
            if len(self.bad_searches.keys()) > 0:
                raise BadQueryTerm(self.bad_searches)
        else:
            study_ids = list(Study.objects.all().values_list("id", flat=True))

        # Make output directory
        if not os.path.exists(self.outdir):
            os.mkdir(self.outdir)

        existing_files = []

        # For each study (name)
        for study_id in study_ids:
            study_id_str = f"study_{study_id:04d}"

            # Make study directory
            study_dir = os.path.join(self.outdir, study_id_str)
            if not os.path.exists(study_dir):
                os.mkdir(study_dir)

            # For each data type
            for data_type in self.data_types:
                datatype_slug = slugify(data_type)

                if data_type in self.zipped_data_types:
                    filepath = os.path.join(
                        study_dir, f"{study_id_str}-{datatype_slug}.zip"
                    )
                else:
                    filepath = os.path.join(
                        study_dir, f"{study_id_str}-{datatype_slug}.tsv"
                    )

                if os.path.exists(filepath) and not self.overwrite:
                    file_exists = FileExistsError(
                        f"File {filepath} exists.  Use the overwrite option to overwrite existing files."
                    )
                    print(
                        f"{trace(file_exists)}\n{type(file_exists).__name__}: {file_exists}"
                    )
                    existing_files.append(filepath)
                    continue

                # A data type name corresponds to a format key
                data_type_key = self.sg.format_name_or_key_to_key(data_type)

                # Construct a query object understood by the format
                # NOTE: This *assumes* every format in self.sg includes Study.id as searchable
                qry = self.sg.create_new_basic_query(
                    "Study",
                    "id",
                    "exact",
                    study_id,
                    data_type_key,
                    "identity",
                    search_again=False,
                )

                # Do the query of the format (ignoring count and optional stats)
                results, _, _ = self.sg.perform_query(qry, data_type_key)

                if data_type in self.zipped_data_types:
                    # Create an AdvancedSearchDownloadMzxmlZIPView instance to prepare the download and pass its
                    # iterator.
                    asdmzv = AdvancedSearchDownloadMzxmlZIPView()
                    asdmzv.prepare_download(qry, res=results)

                    # Output a zip file of files plus a metadata file about the files
                    self.atomic_binary_file_write_and_move(
                        filepath,
                        asdmzv.mzxml_zip_iterator,
                        asdmzv.metadata_content,
                    )

                else:
                    # Compose a list of output lines.  We do this because it's way more efficient to do 1 write
                    # operation on the entire file content than it is to write each line, due to the system calls
                    # involved.
                    content_list = []
                    for line in AdvancedSearchDownloadView.tsv_template_iterator(
                        self.row_template, self.header_template, results, qry, dt_string
                    ):
                        content_list.append(line)

                    # Output a text file
                    self.atomic_text_file_write_and_move(
                        filepath, "".join(content_list)
                    )

        if len(existing_files) > 0:
            nlt = "\n\t"
            raise FileExistsError(
                "The following files exist and were skipped.  You can ignore this error if you do not want to "
                "overwrite these files.  Use the overwrite option to overwrite existing files.\n"
                f"\t{nlt.join(existing_files)}"
            )

    def atomic_text_file_write_and_move(
        self, final_destination_path: str, content: str, encoding="utf-8"
    ):
        """Writes a string to a temporary file and then moves it to the final destination path.

        Uses NamedTemporaryFile to get a temp file in the filesystem.
        It sets delete=False to keep the file after it's closed, so it can be moved.

        Args:
            final_destination_path (str): Path of the file to ultimately output to.
            content (str): One string containing all the file content.
            encoding (str) ["utf-8"]
        Exceptions:
            No explicit exceptions, but some may arise from the file system, like FileExistsError
        Returns:
            None
        """
        # Use NamedTemporaryFile to get a file with a visible name in the filesystem.
        # Set delete=False to keep the file after it's closed, so it can be moved.
        # 'w+t' mode is for text; use 'w+b' for binary data.
        try:
            suffix = os.path.basename(final_destination_path)
            with tempfile.NamedTemporaryFile(
                mode="w+t", delete=False, encoding=encoding, suffix=suffix
            ) as temp_file:
                temp_path = temp_file.name
                # Write data to the temporary file
                temp_file.write(content)
                # File is automatically flushed when exiting the 'with' block

            # Move the file to the final destination.
            os.replace(temp_path, final_destination_path)

            # Print the filepaths to the console as they are exported, so the user can see progress.
            print(final_destination_path)

        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            print(
                f"Cleaned up temporary file {temp_path} due to exception {type(e).__name__}."
            )

            raise e

    def atomic_binary_file_write_and_move(
        self,
        final_destination_path: str,
        iterator: Callable[[str], Iterator[bytes]],
        metadata: str,
    ):
        """Traverses a supplied binary content iterator to write its content to a temporary file and then moves it to
        the final destination path.

        The binary content that is produced by the supplied iterator should be a series of binary files (e.g. zip
        files).  The iterator takes a string containing metadata about the binary files that is written to a (zipped)
        metadata file.  The iterator should zip and include that metadata file among its yields.
        See AdvancedSearchDownloadMzxmlZIPView.mzxml_zip_iterator for an example.

        Uses NamedTemporaryFile to get a temp file in the filesystem.
        It sets delete=False to keep the file after it's closed, so it can be moved.

        Args:
            final_destination_path (str): Path of the file to ultimately output to.
            iterator (Callable[[str], Iterator[bytes]]): An iterator method that takes a metadata string about the
                binary file buffer content it produces.
            metadata (str): A string of metadata about the binary files in the buffer that is supplied as an argument to
                the iterator.
        Exceptions:
            No explicit exceptions, but some may arise from the file system, like FileExistsError
        Returns:
            None
        """
        try:
            suffix = os.path.basename(final_destination_path)
            temp_path = ""
            with tempfile.NamedTemporaryFile(
                mode="w+b", delete=False, suffix=suffix
            ) as temp_file:
                temp_path = temp_file.name
                for content in iterator(metadata):
                    temp_file.write(content)

            # Move the zip to the final destination.
            os.replace(temp_path, final_destination_path)

            # Print the filepaths to the console as they are exported, so the user can see progress.
            print(final_destination_path)

        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            print(
                f"Cleaned up temporary file {temp_path} due to exception {type(e).__name__}."
            )

            raise e


class BadQueryTerm(Exception):
    def __init__(self, bad_searches_dict: dict):
        deets = [f"{k}: {type(v).__name__}: {v}" for k, v in bad_searches_dict.items()]
        nt = "\n\t"
        message = (
            "No study name or ID matches the provided search term(s):\n"
            f"\t{nt.join(deets)}\n"
            "Scroll up to see tracebacks above for each individual exception encountered."
        )
        super().__init__(message)
        self.bad_searches_dict = bad_searches_dict
