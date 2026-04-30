from collections import defaultdict
import hashlib
from itertools import zip_longest
import os
import socket
import tempfile
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, Iterator, List, Optional, Tuple
import zipfile
import io

from django.conf import settings
from django.db.models import Q
from django.template.loader import get_template
from django.utils.text import get_valid_filename

from DataRepo.formats.mzxml_dataformat import MzxmlFormat
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import Study
from DataRepo.utils.exceptions import AggregatedErrors, trace
from DataRepo.utils.export_base import ExportBase
from DataRepo.views.search.download import (
    AdvancedSearchDownloadMzxmlZIPView,
    AdvancedSearchDownloadView,
)


class StudiesExporter(ExportBase):
    """Exports the SearchGroup formats with one file per study and and data type combo.

    Output filenames will be slugified (replacing dashes with underscores) and have the following naming structure:
        {host_name}-{export_datestamp}-{study_name}-{study_id}-{data_type}.{extension}

    Example:
        tb9_pub-2026.04.11-Acute_Stress-0004-mzXML.zip

    The reasoning/value for each filename element:
        host_name (E.g. "tb9" for the tracebase-rabinowitz instance):
            Since TraceBase instances are loaded separately, when users download exported data, including the host
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
        default_host (str): Host/domain name of the TraceBase instance (with dashes replaced with underscores).
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
    default_host = socket.getfqdn().replace("-", "_")

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
        host: Optional[str] = None,  # Defaults to current host/domain
        date: Optional[datetime] = None,  # Defaults to now
        staging_mode=False,
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
        self.staging_mode = staging_mode

        self.host = host if host else self.default_host
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

        self.instance_name = host if host else self.default_host
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

        self.instance_name = host if host else self.default_host
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

        self.instance_name = host if host else self.default_host
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
            if os.path.realpath(settings.DOWNLOADS_DIR) == os.path.realpath(
                self.outdir
            ):
                os.makedirs(self.outdir)
            else:
                os.mkdir(self.outdir)

        existing_files = []

        # For each study (ID/name)
        for study_id, study_name in study_ids_names:
            study_str = f"{self.host}-{export_datestamp}-{study_name}-{study_id:04d}"

            # For each data type
            for data_type in self.data_types:
                suffix = "zip" if data_type in self.zipped_data_types else "tsv"
                filepath = os.path.join(
                    self.outdir, get_valid_filename(f"{study_str}-{data_type}.{suffix}")
                )

                unstaged_filepath = filepath
                if self.staging_mode:
                    filepath += self.staged_ext

                if (
                    os.path.exists(filepath) or os.path.exists(unstaged_filepath)
                ) and not self.overwrite:
                    file_exists = FileExistsError(
                        f"File {unstaged_filepath} exists.  Use the overwrite option to overwrite existing files."
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


class ExportsOrganizer:
    def organize(self, export_dir: str):
        """Takes an export directory containing previously organized^ exports and one unorganized^ export and it
        organizes^ that 1 latest unorganized^ export, as generated by StudiesExporter and output to the export_dir,
        which accumulates exports.  This method should be called immediately after StudiesExporter.export().

        The files contained must be named in the following ways:

        As output by StudiesExporter.export():
            Individually               Filename: {hostname}-{datestamp}-{study_name}-{study_id}-{data_type}.{ext}
        As previously organized by this method:
            All studies and datatypes  Filename: {hostname}-{datestamp}-allstudies-alldatatypes.zip
            All studies by datatype    Filename: {hostname}-{datestamp}-allstudies-{data_type}.zip
            All datatypes by study     Filename: {hostname}-{datestamp}-{study_name}-{study_id}-alldatatypes.zip

        ^ "organize", as used here, as a term, means that it goes through all export files directly in the export_dir
          (i.e. subdirectories are ignored) and determines the last/latest exported versions (by datestamp), and zips
          them in 3 ways:
            All together.
            By study.
            By data type.

        It also leaves the latest individual files and deletes those that only differ from the previous version by
        export date.

        The purpose of doing it this way is so that new exports are not retained if they only differ from previous
        exports by export date.  I.e. An exported file will be deleted if it only differs from the previous export by
        export date.  The guiding strategy is that only unique export versions are retained once they have been
        organized.

        Another aspect of this organization is that it zips all files from the latest export~ together, all files of a
        single datatype together, and all files of a single study together.  This is a convenience feature for users so
        that they can select the type and amount of data they want in a single download click on the downloads page
        where these exports will be served.

        ~ "Latest export" means the last study/datatype that differs from the previous export.  That may not be the
          latest date in any one case.  E.g. If study "A" had no change to its FCirc data in the latest export, that
          'Study A FCirc' file is deleted and the file used for zipping in various combinations falls back to the
          previous version exported the week prior.

        If any individual file from the last export has changed, all combination zip files it is included in will be re-
        zipped with a new date (date is a stand-in for version).

        Example:
            Exported files prior to organization:
                tracebase-2026.04.17-acute_stress-0035-PeakData.tsv
                tracebase-2026.04.17-acute_stress-0035-PeakGroups.tsv
                tracebase-2026.04.17-acute_stress-0035-FCirc.tsv
                tracebase-2026.04.17-acute_stress-0035-mzXMLs.zip
                tracebase-2026.04.17-acute_stress-0035-alldatatypes.zip  # By study, 2026.04.17
                tracebase-2026.04.17-allstudies-alldatatypes.zip         # All together, 2026.04.17
                tracebase-2026.04.17-allstudies-PeakData.zip             # By data type, 2026.04.17
                tracebase-2026.04.17-allstudies-PeakGroups.zip           # By data type, 2026.04.17
                tracebase-2026.04.17-allstudies-FCirc.zip                # By data type, 2026.04.17
                tracebase-2026.04.17-allstudies-mzXMLs.zip               # By data type, 2026.04.17
                tracebase-2026.04.24-acute_stress-0035-PeakData.tsv      # Same - will be deleted
                tracebase-2026.04.24-acute_stress-0035-PeakGroups.tsv    # Same - will be deleted
                tracebase-2026.04.24-acute_stress-0035-FCirc.tsv         # Differs from the 2026.04.17 version
                tracebase-2026.04.24-acute_stress-0035-mzXMLs.zip        # Same - will be deleted
            Exported files after organization:
                tracebase-2026.04.17-acute_stress-0035-PeakData.tsv      # Included in the 2026.04.24 zips
                tracebase-2026.04.17-acute_stress-0035-PeakGroups.tsv    # Included in the 2026.04.24 zips
                tracebase-2026.04.17-acute_stress-0035-FCirc.tsv         # NOT included in the 2026.04.24 zips
                tracebase-2026.04.17-acute_stress-0035-mzXMLs.zip        # Included in the 2026.04.24 zips
                tracebase-2026.04.17-acute_stress-0035-alldatatypes.zip
                tracebase-2026.04.17-allstudies-alldatatypes.zip
                tracebase-2026.04.17-allstudies-PeakData.zip
                tracebase-2026.04.17-allstudies-PeakGroups.zip
                tracebase-2026.04.17-allstudies-FCirc.zip
                tracebase-2026.04.17-allstudies-mzXMLs.zip
                tracebase-2026.04.24-acute_stress-0035-FCirc.tsv         # Only one kept from the latest export
                tracebase-2026.04.24-allstudies-alldatatypes.zip         # New file
                tracebase-2026.04.24-allstudies-PeakData.zip             # New file
                tracebase-2026.04.24-allstudies-PeakGroups.zip           # New file
                tracebase-2026.04.24-allstudies-FCirc.zip                # New file
                tracebase-2026.04.24-allstudies-mzXMLs.zip               # New file
                tracebase-2026.04.24-acute_stress-0035-alldatatypes.zip  # New file
        Args:
            export_dir: str
        Exceptions:
            None
        Returns:
            None
        """
        files = [f for f in os.listdir(export_dir) if os.path.isfile(f)]
        (by_study_first, by_datatype_first) = self.organize_exports_by_study_and_datatype(files)
        last_date, last_date_by_study, last_date_by_datatype = self.remove_unchanged_exports(by_study_first)
        self.zip_export_combos(by_study_first, by_datatype_first, last_date, last_date_by_study, last_date_by_datatype)

    def organize_exports_by_study_and_datatype(self, files):
        """Takes a set of export files that includes previously organized^ exports and one unorganized^ series of export
        files (e.g. the files recently generated by StudiesExporter.export()) and it returns 2 dicts containing metadata
        of those files organized by study then data type and by data type then study.

        Args:
            files (List[str]): List of filepaths of previously organized and new export files.
        Exceptions:
            None
        Returns:
            None
        """
        study_names = dict((rec.id, rec.name) for rec in Study.objects.all())
        study_then_datatype_data: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        datatype_then_study_data: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for file in sorted(files):
            filenameext = os.path.basename(file)
            filename: str
            filename, ext = os.path.splitext(filenameext)
            (host, date_str, slugged_study_name, study_id, data_type) = filename.split("-")
            study_then_datatype_data[host][study_id][data_type].append(
                {
                    "date": date_str,
                    "file": file,
                    "id": str(int(study_id)),  # Removes leading zeroes
                    "name": study_names[study_id],
                    "slug": slugged_study_name,
                    "ext": ext,
                }
            )
            datatype_then_study_data[host][data_type][study_id].append(
                {
                    "date": date_str,
                    "file": file,
                    "id": str(int(study_id)),
                    "name": study_names[study_id],
                    "slug": slugged_study_name,
                    "ext": ext,
                }
            )
        return study_then_datatype_data, datatype_then_study_data

    def remove_unchanged_exports(self, by_study_first: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]):
        last_date = "1972.11.24"
        last_date_by_study: Dict[str, str] = {}
        last_date_by_datatype: Dict[str, str] = {}
        for host_dict in by_study_first.values():
            for study_id, study_dict in host_dict.items():
                for data_type, file_dict_list in study_dict.items():
                    if len(file_dict_list) < 2:
                        continue
                    # file_dict_list is already sorted by date because the files are sorted and the name starts with (the host, then) the date
                    prev_file_dict = file_dict_list[-2]
                    last_file_dict = file_dict_list[-1]
                    if not self.files_differ(prev_file_dict, last_file_dict, zip = last_file_dict["ext"] == "zip"):
                        # Delete the last file and set the last dict's file to the previous
                        os.remove(file_dict_list[-1]["file"])
                        file_dict_list.pop()
                    if file_dict_list[-1]["date"] > last_date:
                        last_date = file_dict_list[-1]["date"]
                    if study_id not in last_date_by_study or file_dict_list[-1]["date"] > last_date_by_study[study_id]:
                        last_date_by_study[study_id] = file_dict_list[-1]["date"]
                    if data_type not in last_date_by_datatype or file_dict_list[-1]["date"] > last_date_by_datatype[data_type]:
                        last_date_by_datatype[data_type] = file_dict_list[-1]["date"]
        return last_date, last_date_by_study, last_date_by_datatype


    def files_differ(self, prev_file_dict, last_file_dict, zip=False):
        if zip:
            return self.mzxml_zips_differ(prev_file_dict, last_file_dict)
        return self.tsv_files_differ(prev_file_dict, last_file_dict)

    def mzxml_zips_differ(self, prev_file_dict, next_file_dict):
        """Calls self.tsv_file_objs_differ on the TSV files contained in the mzXML zip files."""
        differs = False

        with zipfile.ZipFile(prev_file_dict["file"], 'r') as zp:
            tsv_files = [name for name in zp.namelist() if name.endswith('.tsv')]
            if len(tsv_files) != 1:
                raise
            prev_tsv = tsv_files[0]

            with zipfile.ZipFile(next_file_dict["file"], 'r') as zn:
                tsv_files = [name for name in zn.namelist() if name.endswith('.tsv')]
                if len(tsv_files) != 1:
                    raise
                next_tsv = tsv_files[0]


                # Access the member as a binary file-like object
                with zp.open(prev_tsv) as prev_binary_file:
                    # Wrap the binary stream to handle text decoding
                    with io.TextIOWrapper(prev_binary_file, encoding='utf-8') as prev_file_obj:

                        with zn.open(prev_tsv) as next_binary_file:
                            # Wrap the binary stream to handle text decoding
                            with io.TextIOWrapper(next_binary_file, encoding='utf-8') as next_file_obj:
                                differs = self.tsv_file_objs_differ(prev_file_obj, next_file_obj)
                                if differs:
                                    return differs

        # If the tsvs are the same, also check the mzXML files
        prev_mzxml_checksums = self.get_mzxml_zip_checksums(prev_file_dict["file"])
        next_mzxml_checksums = self.get_mzxml_zip_checksums(next_file_dict["file"])

        return prev_mzxml_checksums.items() == next_mzxml_checksums.items()

    def tsv_files_differ(self, prev_file_dict, last_file_dict):
        with open(prev_file_dict["file"], 'r') as f1, open(last_file_dict["file"], 'r') as f2:
            return self.tsv_file_objs_differ(f1, f2)

    def tsv_file_objs_differ(self, prev_file_obj, next_file_obj):

        # Create generators that skip comments
        prev_file_generator = (line for line in prev_file_obj if not str(line).startswith('#'))
        next_file_generator = (line for line in next_file_obj if not str(line).startswith('#'))

        # compare line by line; return False immediately if a mismatch is found
        for prev_file_line, next_file_line in zip_longest(prev_file_generator, next_file_generator):
            if prev_file_line != next_file_line:
                return True

        return False

    def get_mzxml_zip_checksums(self, zip_path):
        checksums = {}
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Filter files by extension
            mzxml_files = [f for f in z.namelist() if f.lower().endswith(".mzxml")]

            for file_name in mzxml_files:
                # Open file in-memory without extracting to disk
                with z.open(file_name) as f:
                    sha256_hash = hashlib.sha256()
                    # Read in chunks for memory efficiency with large files
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)
                    checksums[file_name] = sha256_hash.hexdigest()

        return checksums

    def zip_export_combos(
        self,
        by_study_first: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]],
        by_datatype_first: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]],
        last_date: str,
        last_date_by_study: Dict[str, str],
        last_date_by_datatype: Dict[str, str],
    ):
        # Zip everything
        # Orig Filename: {hostname}-{datestamp}-{study_name}-{study_id}-{data_type}.{extension}

        # All studies and datatypes  Filename: {hostname}-{datestamp}-allstudies-alldatatypes.zip
        # All studies by datatype    Filename: {hostname}-{datestamp}-allstudies-{data_type}.zip
        # All datatypes by study     Filename: {hostname}-{datestamp}-{study_name}-{study_id}-alldatatypes.zip
        # Individually               Filename: {hostname}-{datestamp}-{study_name}-{study_id}-{data_type}.zip

        slugified_study_names = dict(
            (
                rec.id,
                get_valid_filename(rec.name).replace("-", "_"),
            )
            for rec in Study.objects.all()
        )

        # Create the filenames first, then create the zip if it does not exist
        for host, host_dict in by_study_first.items():
            allstudies_alldatatypes_file = f"{host}-{last_date}-allstudies-alldatatypes.zip"
            if not os.path.exists(allstudies_alldatatypes_file):
                self.create_all_zip(host_dict, allstudies_alldatatypes_file)

            for study_id, study_dict in host_dict.items():
                study_file = "-".join(
                    host,
                    last_date_by_study[study_id],
                    slugified_study_names[study_id],
                    "alldatatypes.zip"
                )
                if not os.path.exists(study_file):
                    self.create_study_zip(study_dict, study_file)

            for data_type, data_type_dict in by_datatype_first.items():
                datatype_file = "-".join(
                    host,
                    last_date_by_datatype[data_type],
                    "allstudies",
                    f"{data_type}.zip"
                )
                if not os.path.exists(datatype_file):
                    self.create_datatype_zip(data_type_dict, datatype_file)

    def create_all_zip(self, data: Dict[str, Dict[str, List[Dict[str, str]]]], filepath: str):
        """Takes dict of export files sorted by study and data type and creates a flat zip file of all its files.

        Args:
            data (Dict[str, Dict[str, List[Dict[str, str]]]]): A dict keyed on study ID and data type containing lists
                of dicts containing metadata about export files.  The keys of the dicts in the lists: date, file, id,
                name, slug, and ext.
            filepath (str): The output zip file path/name.
        Exceptions:
            None
        Returns:
            None
        """
        self.filepath_list_to_zip(
            [
                export_metadata_dict["file"]
                for data_type_dict in data.values()
                for metadata_list in data_type_dict.values()
                for export_metadata_dict in metadata_list
            ],
            filepath,
        )

    def create_study_zip(self, data: Dict[str, List[Dict[str, str]]], filepath: str):
        """Takes dict of export files sorted by data type and creates a flat zip file of all its files.

        Args:
            data (Dict[str, List[Dict[str, str]]]): A dict keyed on data type containing lists of dicts containing
                metadata about export files.  The keys of the dicts in the lists: date, file, id, name, slug, and ext.
            filepath (str): The output zip file path/name.
        Exceptions:
            None
        Returns:
            None
        """
        self.filepath_list_to_zip(
            [
                export_metadata_dict["file"]
                for metadata_list in data.values()
                for export_metadata_dict in metadata_list
            ],
            filepath,
        )

    def create_datatype_zip(self, data: Dict[str, List[Dict[str, str]]], filepath: str):
        """Takes dict of export files sorted by study ID and creates a flat zip file of all its files.

        Args:
            data (Dict[str, List[Dict[str, str]]]): A dict keyed on study ID containing lists of dicts containing
                metadata about export files.  The keys of the dicts in the lists: date, file, id, name, slug, and ext.
            filepath (str): The output zip file path/name.
        Exceptions:
            None
        Returns:
            None
        """
        self.filepath_list_to_zip(
            [
                export_metadata_dict["file"]
                for metadata_list in data.values()
                for export_metadata_dict in metadata_list
            ],
            filepath,
        )

    def filepath_list_to_zip(self, file_paths: List[str], zip_filepath: str):
        common_base = os.path.commonpath(file_paths)
        with zipfile.ZipFile(zip_filepath, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                # Calculate the path relative to the common base
                arcname = os.path.relpath(file_path, start=common_base)
                # Use arcname to avoid storing the full absolute path in the zip
                zipf.write(file_path, arcname=arcname)


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
