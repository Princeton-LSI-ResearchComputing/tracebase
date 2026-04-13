import os
import socket
import tempfile
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, Iterator, List, Optional, Tuple

from django.db.models import Q
from django.template.loader import get_template
from django.utils.text import get_valid_filename

from DataRepo.formats.mzxml_dataformat import MzxmlFormat
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import Study
from DataRepo.utils.exceptions import AggregatedErrors, trace
from DataRepo.views.search.download import (
    AdvancedSearchDownloadMzxmlZIPView,
    AdvancedSearchDownloadView,
)


class StudiesExporter:
    """Exports the SearchGroup formats with one file per study and and data type combo.

    Output filenames will be slugified (replacing dashes with underscores) and have the following naming structure:
        {instance_name}-{export_datestamp}-{study_name}-{study_id}-{data_type}.{extension}

    Example:
        tb9-pub-2026.04.11-Acute_Stress-0004-mzXML.zip

    The reasoning/value for each filename element:
        instance_name (E.g. "tb9" for the tracebase-rabinowitz instance):
            Since TraceBase instances are loaded separately, when users download exported data, including the instance
            name can be used to differentiate between downloads from different instances.  They should theoretically be
            identical for the same study, but if any data is manually edited, knowing the source can be critical.
        export_datestamp (E.g. "2026.04.11"):
            This is the date of the export.  This serves as a version number for the export.
        study_name (E.g. "Acute_Stress"):
            Note that study names may contain dashes.  The study name is slugified, so it will not necessarily exactly
            match the study's name as displayed in TraceBase.
        study_id (E.g. "0004"):
            This is the internal database ID of the study.
        data_type (E.g. "mzXML"):
            This is the name of the SearchGroup format
        extension (E.g. "tsv"):
            There are 2 extensions currently: tsv and zip.  The zip extension is specific to the mzXML search format.

    Class Attributes:
        sg (SearchGroup): This defines the data types and is the means by which queries are executed.
        all_data_types (List[str]): These are the names of all of the DataFormat objects contained by sg.
        all_zipped_data_types (List[str]):  This is the subset of all_data_types that should be exported as zip files.
        header_template (Template): Used to render the commented metadata header of exported TSV files.
        row_template (Template): Used to render the content of the exported TSV files.
        datestamp_format (str): The date string used in the exported filenames.
        default_instance (str): Hostname of the TraceBase instance (with dashes replaced with underscores).
    Instance Attributes:
        bad_searches (Dict[str, Exception]): Query exceptions by study ID or name.
        outdir (str): Output directory.
        study_targets (List[str]): List of study IDs and/or names.
        data_types (List[str]): The data types to be exported.  Must be a subset of cls.all_data_types.
        zipped_data_types (List[str]): The zipped data types to be exported.  Must be a subset of data_types.
        overwrite (bool) [False]: Whether to overwrite existing exported files.
    """

    sg = SearchGroup()
    all_data_types = [fmtobj.name for fmtobj in sg.modeldata.values()]
    all_zipped_data_types = [MzxmlFormat.name]
    header_template = get_template("search/downloads/download_header.tsv")
    row_template = get_template("search/downloads/download_row.tsv")
    default_instance = socket.gethostname().replace("-", "_")

    # NOTE: datestamp_format intentionally differs from AdvancedSearchDownloadView.datestamp_format in that it does not
    # include the time (since the intention is to run the export in a cron less than or equal to once a day) and we
    # would like the dates to sort chronologically (i.e. numeric year-month-day)
    datestamp_format = "%Y.%m.%d"

    def __init__(
        self,
        outdir: str,
        study_targets: Optional[List[str]] = None,
        data_types: Optional[List[str]] = None,
        overwrite: bool = False,
        host: Optional[str] = None,
        date: Optional[datetime] = None,
    ):
        self.bad_searches: Dict[str, int] = {}

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

        self.instance_name = host if host else self.default_instance
        self.date = date

        # A script on a cron-job uses the study ID in the file name to compare exported files with previously exported
        # versions.  It does this by splitting on dash and taking the study ID from the file name, relative to the end
        # of the file, thus the format value at the end of the file name may not have dashes.
        if any("-" in datatype_name for datatype_name in self.all_data_types):
            bad_format_names = [dtn for dtn in self.all_data_types if "-" in dtn]
            raise ValueError(
                "The following SearchGroup format names contain dashes ('-') which are not allowed in order to parse "
                f"export file names: {bad_format_names}."
            )

    def export(self):
        # For individual traceback prints
        aes = AggregatedErrors()

        # Export time for the outfile headers
        if self.date:
            export_time = self.date
        else:
            export_time = datetime.now()

        dt_string = export_time.strftime(AdvancedSearchDownloadView.date_format)

        # Export time for the outfile name
        export_datestamp = export_time.strftime(self.datestamp_format)

        # Identify the study records to export (by name)
        study_ids_names = []
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
                    study_ids_names.append(
                        (
                            study_rec.id,
                            get_valid_filename(study_rec.name.replace("-", "_")),
                        )
                    )
                except Exception as e:
                    # Buffering exception to just print the traceback
                    aes.buffer_error(e)
                    # Collect the exceptions for an easier to debug and more succinct exception to raise
                    self.bad_searches[study_target] = e

            # Summarize the encountered query issues with useful info not provided in the original exceptions
            if len(self.bad_searches.keys()) > 0:
                raise BadQueryTerm(self.bad_searches)
        else:
            study_ids_names = list(
                (
                    stdy.id,
                    get_valid_filename(stdy.name.replace("-", "_")),
                )
                for stdy in Study.objects.all()
            )

        self.check_study_names(study_ids_names)

        # Make output directory
        if not os.path.exists(self.outdir):
            os.mkdir(self.outdir)

        existing_files = []

        # For each study (ID/name)
        for study_id, study_name in study_ids_names:
            study_str = (
                f"{self.instance_name}-{export_datestamp}-{study_name}-{study_id:04d}"
            )

            # For each data type
            for data_type in self.data_types:
                if data_type in self.zipped_data_types:
                    filepath = os.path.join(
                        self.outdir, get_valid_filename(f"{study_str}-{data_type}.zip")
                    )
                else:
                    filepath = os.path.join(
                        self.outdir, get_valid_filename(f"{study_str}-{data_type}.tsv")
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

    def check_study_names(self, study_ids_names: List[Tuple[int, str]]):
        """This checks the sanitized study names for uniqueness"""
        unique_study_names = []
        dupe_study_names: Dict[str, int] = defaultdict(int)
        for _, study_name in study_ids_names:
            if study_name in unique_study_names:
                if study_name in dupe_study_names:
                    dupe_study_names[study_name] += 1
                else:
                    dupe_study_names[study_name] = 2
            else:
                unique_study_names.append(study_name)
        if dupe_study_names:
            raise DuplicateSlugifiedStudyNames(dupe_study_names)


class BadQueryTerm(Exception):
    def __init__(self, bad_searches: Dict[str, Exception]):
        deets = [f"{k}: {type(v).__name__}: {v}" for k, v in bad_searches.items()]
        nt = "\n\t"
        message = (
            "No study name or ID matches the provided search term(s):\n"
            f"\t{nt.join(deets)}\n"
            "Scroll up to see tracebacks above for each individual exception encountered."
        )
        super().__init__(message)
        self.bad_searches = bad_searches


class DuplicateSlugifiedStudyNames(Exception):
    def __init__(self, dupe_study_names: Dict[str, int]):
        message = f"These slugified study names are not unique: {list(dupe_study_names.keys())}."
        super().__init__(message)
        self.dupe_study_names = dupe_study_names
